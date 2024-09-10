package main

import (
	"context"
	"example/flow-key-indexer/pkg/pg"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/access/grpc"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Params struct {
	LogLevel               string `default:"info"`
	Port                   string `default:"8080"`
	FlowUrl1               string `default:"access.mainnet.nodes.onflow.org:9000"`
	FlowUrl2               string
	FlowUrl3               string
	FlowUrl4               string
	AllFlowUrls            []string `ignored:"true"`
	ChainId                string   `default:"flow-mainnet"`
	MaxAcctKeys            int      `default:"1000"`
	BatchSize              int      `default:"50000"`
	IgnoreZeroWeight       bool     `default:"true"`
	IgnoreRevoked          bool     `default:"true"`
	WaitNumBlocks          int      `default:"200"`
	BlockPolIntervalSec    int      `default:"180"`
	SyncDataPolIntervalMin int      `default:"10"`
	SyncDataStartIndex     int      `default:"30000000"`
	MaxBlockRange          int      `default:"600"`
	FetchSlowDownMs        int      `default:"500"`
	PurgeOnStart           bool     `default:"false"`
	EnableSyncData         bool     `default:"true"`
	EnableIncremental      bool     `default:"true"`

	PostgreSQLHost              string        `default:"localhost"`
	PostgreSQLPort              uint16        `default:"5432"`
	PostgreSQLUsername          string        `default:"postgres"`
	PostgreSQLPassword          string        `required:"false"`
	PostgreSQLDatabase          string        `default:"keyindexer"`
	PostgreSQLSSL               bool          `default:"true"`
	PostgreSQLLogQueries        bool          `default:"false"`
	PostgreSQLSetLogger         bool          `default:"false"`
	PostgreSQLRetryNumTimes     uint16        `default:"30"`
	PostgreSQLRetrySleepTime    time.Duration `default:"1s"`
	PostgreSQLPoolSize          int           `default:"1"`
	PostgresLoggerPrefix        string        `default:"keyindexer"`
	PostgresPrometheusSubSystem string        `default:"keyindexer"`
}

type App struct {
	DB         *pg.Store
	flowClient *FlowAdapter
	p          Params
	dataLoader *DataLoader
	rest       *Rest
}

type ClientWrapper struct {
	*grpc.BaseClient
}

func (cw *ClientWrapper) ExecuteScriptAtBlockHeight(ctx context.Context, blockHeight uint64, script []byte, arguments []cadence.Value) (cadence.Value, error) {
	return cw.BaseClient.ExecuteScriptAtBlockHeight(ctx, blockHeight, script, arguments)
}

func (a *App) Initialize(params Params) {
	params.AllFlowUrls = setAllFlowUrls(params)
	a.p = params

	dbConfig := getPostgresConfig(params, log.Logger)

	db := pg.NewStore(dbConfig, log.Logger)
	err := db.Start(params.PurgeOnStart)
	if err != nil {
		log.Fatal().Err(err).Msg("Database could not be found or created")
	}
	a.DB = db

	a.flowClient = NewFlowClient(strings.TrimSpace(a.p.FlowUrl1))
	a.dataLoader = NewDataLoader(*a.DB, *a.flowClient, params)
	a.rest = NewRest(*a.DB, *a.flowClient, params)
}

func (a *App) Run() {
	bufferSize := 1000
	ctx := context.Background()
	highPriChan := make(chan []flow.Address)
	lowPriAddressChan := make(chan []flow.Address, bufferSize)
	currentBlock, err := a.flowClient.Client.GetLatestBlockHeader(context.Background(), true)

	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
	}
	if currentBlock == nil {
		log.Error().Msg("Could not get current block height")
	}
	if currentBlock.Height == 0 {
		log.Error().Msg("Could not get current block height")
	}

	startingBlockHeight := currentBlock.Height - uint64(a.p.MaxBlockRange)

	a.DB.UpdateLoadedBlockHeight(startingBlockHeight)

	log.Debug().Msgf("Current block from server %v", currentBlock.Height)

	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
	}

	// start up process to handle addresses that are put in addressChan channel
	ProcessAddressChannels(ctx, log.Logger, a.flowClient.Client,
		highPriChan, lowPriAddressChan,
		a.DB.InsertPublicKeyAccounts, a.p)

	if a.p.EnableSyncData {
		log.Info().Msgf("Data Sync service is enabled")
		go a.bulkLoad(lowPriAddressChan)
	}

	if a.p.EnableIncremental {
		log.Info().Msgf("Incremental service is enabled")
		go a.loadIncrementalData(highPriChan)
	}
	go a.waitForChannelsToUpdateDistinct(ctx, highPriChan, lowPriAddressChan, time.Duration(a.p.SyncDataPolIntervalMin)*time.Minute, a.DB.UpdateDistinctCount)
	a.rest.Start()
}

func (a *App) loadIncrementalData(addressChan chan []flow.Address) {
	// Kick off the incremental load first
	a.incrementalLoad(addressChan)

	ticker := time.NewTicker(time.Duration(a.p.BlockPolIntervalSec) * time.Second)

	go func() {
		for range ticker.C {
			a.incrementalLoad(addressChan)
		}
	}()
}

func (a *App) waitForAddressChanToReduce(addressChan chan []flow.Address, pause time.Duration) {
	ticker := time.NewTicker(pause)
	defer ticker.Stop()

	for range ticker.C {
		log.Debug().Msgf("Address channel size %d", len(addressChan))
		if len(addressChan) < 10 {
			log.Debug().Msg("Address channel has cleared enough, run another bulk loader")
			return
		}
	}
}

func (a *App) waitForChannelsToUpdateDistinct(ctx context.Context, highChan chan []flow.Address, lowChan chan []flow.Address, pause time.Duration, updateDistinctCount func()) {
	ticker := time.NewTicker(pause)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Service is stopping, exiting waitForChannelsToUpdateDistinct")
			return
		case <-ticker.C:
			log.Debug().Msgf("High-priority channel size %d", len(highChan))
			log.Debug().Msgf("Low-priority channel size %d", len(lowChan))

			if len(highChan) < 10 && len(lowChan) < 10 {
				log.Debug().Msg("Both channels have cleared enough, run another bulk loader")
				updateDistinctCount()
			}
		}
	}
}

func (a *App) bulkLoad(lowPrioAddressChan chan []flow.Address) {
	ctx := context.Background()
	startIndex := uint(a.p.SyncDataStartIndex)
	pause := time.Duration(a.p.FetchSlowDownMs) * time.Millisecond
	// continuously run the bulk load process
	for {
		start := time.Now()
		currentBlock, err := a.flowClient.Client.GetLatestBlockHeader(ctx, true)
		if err != nil {
			log.Error().Err(err).Msg("Bulk Could not get current block height from default flow client")
		}

		log.Info().Msgf("Bulk Start Load, %v", currentBlock.Height)

		ap, errLoad := InitAddressProvider(ctx, log.Logger, flow.ChainID(a.p.ChainId), currentBlock.ID, a.flowClient.Client, pause, startIndex)
		if errLoad != nil {
			log.Error().Err(errLoad).Msg("Bulk, could not initialize address provider")
		}
		// set start index based on found address last index
		startIndex = ap.lastAddressIndex
		log.Debug().Msgf("Bulk Last address index %d", startIndex)
		ap.GenerateAddressBatches(lowPrioAddressChan, a.p.BatchSize)

		duration := time.Since(start)
		log.Info().Msgf("Bulk End Load, duration %f min, %v", duration.Minutes(), currentBlock.Height)

		// wait for the address channel to reduce before running another bulk load
		a.waitForAddressChanToReduce(lowPrioAddressChan, pause)
		// Add a delay if needed to prevent it from running too frequently
		time.Sleep(time.Duration(a.p.SyncDataPolIntervalMin) * time.Minute) // Adjust the sleep duration as needed
	}
}

func (a *App) incrementalLoad(addressChan chan []flow.Address) {
	start := time.Now()
	loadedBlkHeight, _ := a.DB.GetLoadedBlockHeight()
	currentHeight, errCurr := a.flowClient.GetCurrentBlockHeight()
	blockRange := currentHeight - loadedBlkHeight
	if errCurr != nil {
		log.Error().Err(errCurr).Msg("Inc could not get current block height")
		return
	}

	log.Info().Msgf("Inc Start Load, from %d, to: %d, to load %d", loadedBlkHeight, currentHeight, blockRange)

	var synchToBlockHeight uint64
	var err error
	synchToBlockHeight, err = a.dataLoader.RunIncAddressesLoader(addressChan, loadedBlkHeight, currentHeight)
	if err != nil {
		log.Error().Err(err).Msg("Inc could not load incremental public keys, will retry if falling behind ")
	}
	duration := time.Since(start)

	if err == nil {
		a.DB.UpdateLoadedBlockHeight(synchToBlockHeight)
	}

	log.Info().Msgf("Inc Finish Load, %f sec, from: %d to: %d, loaded %d", duration.Seconds(), loadedBlkHeight, synchToBlockHeight, synchToBlockHeight-loadedBlkHeight)

	currentBlockHeight, _ := a.flowClient.GetCurrentBlockHeight()
	// check if processing events took too long to wait for another interval and run an extra incremental load
	newBlockRange := currentBlockHeight - synchToBlockHeight
	if newBlockRange > uint64(a.p.WaitNumBlocks) {
		refreshBlock := currentBlockHeight - uint64(a.p.MaxBlockRange)
		log.Warn().Msgf("Inc load is lagging, running incremental at %d, %d blocks", refreshBlock, newBlockRange)
		synchToBlockHeight, _ = a.dataLoader.RunIncAddressesLoader(addressChan, refreshBlock, currentBlockHeight)
		a.DB.UpdateLoadedBlockHeight(synchToBlockHeight)
	}
}

func setAllFlowUrls(params Params) []string {
	var all []string
	all = processUrl(params.FlowUrl1, all)
	all = processUrl(params.FlowUrl2, all)
	all = processUrl(params.FlowUrl3, all)
	all = processUrl(params.FlowUrl4, all)
	return all
}

func processUrl(url string, collection []string) []string {
	newUrl := strings.TrimSpace(url)
	if newUrl != "" {
		collection = append(collection, newUrl)
	}
	return collection
}

func getPostgresConfig(conf Params, logger zerolog.Logger) pg.DatabaseConfig {
	return pg.DatabaseConfig{
		Host:     conf.PostgreSQLHost,
		Password: conf.PostgreSQLPassword,
		Name:     conf.PostgreSQLDatabase,
		User:     conf.PostgreSQLUsername,
		Port:     int(conf.PostgreSQLPort),
	}
}
