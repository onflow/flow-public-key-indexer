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
	IgnoreRevoked          bool     `default:"false"`
	WaitNumBlocks          int      `default:"200"`
	BlockPolIntervalSec    int      `default:"180"`
	SyncDataPolIntervalMin int      `default:"1"`
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

	dbConfig := getPostgresConfig(params)

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
	ctx := context.Background()
	highPriChan := make(chan []flow.Address)
	lowPriAddressChan := make(chan []flow.Address)
	currentBlock, err := a.flowClient.Client.GetLatestBlockHeader(context.Background(), true)

	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
		return
	}
	if currentBlock == nil {
		log.Error().Msg("Could not get current block height")
		return
	}
	if currentBlock.Height == 0 {
		log.Error().Msg("Could not get current block height")
		return
	}

	startingBlockHeight := currentBlock.Height - uint64(a.p.MaxBlockRange)

	a.DB.UpdateLoadedBlockHeight(startingBlockHeight)

	log.Debug().Msgf("Current block from server %v", currentBlock.Height)

	// start up process to handle addresses that are put in addressChan channel
	ProcessAddressChannels(ctx,
		log.Logger,
		a.flowClient.Client,
		highPriChan,
		lowPriAddressChan,
		a.DB,
		a.p)
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
	batchSize := a.p.BatchSize
	ignoreList := []string{}
	maxWaitTime := time.Duration(a.p.SyncDataPolIntervalMin) * time.Minute

	for {
		start := time.Now()

		// Fetch addresses to process
		addresses, err := a.DB.GetAccountsToProcess(batchSize, ignoreList)
		fetch := time.Since(start)
		log.Debug().Msgf("Bulk Fetch Addresses, duration %.2f min", fetch.Seconds())

		if err != nil {
			log.Error().Err(err).Msg("Bulk Could not get unique addresses without algos")
			time.Sleep(time.Minute)
			continue
		}

		if len(addresses) == 0 {
			log.Info().Msg("Bulk No new addresses to process, waiting before next attempt")
			time.Sleep(maxWaitTime)
			continue
		}

		log.Debug().Msgf("Bulk addresses to process %d ", len(addresses))

		flowAddresses := make([]flow.Address, len(addresses))
		for i, address := range addresses {
			flowAddresses[i] = flow.HexToAddress(address)
		}

		// Try to send addresses to channel with a timeout
		select {
		case lowPrioAddressChan <- flowAddresses:
		case <-time.After(30 * time.Second):
			log.Warn().Msg("Bulk Channel full, skipping this batch")
			continue
		}

		// Wait for the channel to clear or timeout
		waitStart := time.Now()
		for len(lowPrioAddressChan) > 0 {
			if time.Since(waitStart) > maxWaitTime {
				log.Warn().Msg("Bulk Max wait time exceeded, continuing to next iteration")
				break
			}
			time.Sleep(time.Second)
		}

		// After the channel clears, remove the addresses from the addressprocessing table
		removeStart := time.Now()
		err = a.DB.RemoveAccountsProcessing(addresses)
		removeDuration := time.Since(removeStart)
		if err != nil {
			log.Error().Err(err).Msg("Bulk Could not remove processed addresses")
		} else {
			log.Debug().Msgf("Bulk Removed %d processed addresses, duration %.2f min", len(addresses), removeDuration.Minutes())
		}

		duration := time.Since(start)
		log.Info().Msgf("Bulk End Load, duration %.2f min, fetched %d addresses", duration.Minutes(), len(addresses))
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

func getPostgresConfig(conf Params) pg.DatabaseConfig {
	return pg.DatabaseConfig{
		Host:     conf.PostgreSQLHost,
		Password: conf.PostgreSQLPassword,
		Name:     conf.PostgreSQLDatabase,
		User:     conf.PostgreSQLUsername,
		Port:     int(conf.PostgreSQLPort),
	}
}
