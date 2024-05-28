package main

import (
	"context"
	"example/flow-key-indexer/pkg/pg"
	"strings"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Params struct {
	LogLevel            string `default:"info"`
	Port                string `default:"8080"`
	FlowUrl1            string `default:"access.mainnet.nodes.onflow.org:9000"`
	FlowUrl2            string
	FlowUrl3            string
	FlowUrl4            string
	AllFlowUrls         []string `ignored:"true"`
	ChainId             string   `default:"flow-mainnet"`
	MaxAcctKeys         int      `default:"1000"`
	BatchSize           int      `default:"100"`
	IgnoreZeroWeight    bool     `default:"true"`
	IgnoreRevoked       bool     `default:"true"`
	WaitNumBlocks       int      `default:"200"`
	BlockPolIntervalSec int      `default:"180"`
	MaxBlockRange       int      `default:"600"`
	FetchSlowDownMs     int      `default:"50"`
	PurgeOnStart        bool     `default:"false"`
	EnableSyncData      bool     `default:"true"`
	EnableIncremental   bool     `default:"true"`

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
	addressChan := make(chan []flow.Address)
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
	log.Info().Msgf("Load, %d start block %d \n", currentBlock.Height, startingBlockHeight)
	a.DB.UpdatePendingBlockHeight(startingBlockHeight) //186356930
	a.DB.UpdateUpdatedBlockHeight(startingBlockHeight) //186356881
	log.Debug().Msgf("Current block from server %v", currentBlock)
	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
	}

	ProcessAddressChannel(context.Background(), log.Logger, a.flowClient.Client, a.p.BlockPolIntervalSec, addressChan, a.DB.InsertPublicKeyAccounts)
	if a.p.EnableSyncData {
		log.Info().Msgf("Data Sync service is enabled")

		go func() { a.loadPublicKeyData(addressChan, currentBlock) }()
	}

	if a.p.EnableIncremental {
		log.Info().Msgf("Incremental service is enabled")
		go a.loadIncrementalData(addressChan)
	}

	a.rest.Start()
}

func (a *App) loadIncrementalData(addressChan chan []flow.Address) {
	// Kick off the incremental load first
	a.incrementalLoad(addressChan)

	ticker := time.NewTicker(time.Duration(a.p.BlockPolIntervalSec) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				go func() {
					a.incrementalLoad(addressChan)
				}()
			case <-quit:
				log.Info().Msg("ticket is stopped")
				ticker.Stop()
				return
			}
		}
	}()
}

func (a *App) loadPublicKeyData(addressChan chan []flow.Address, currentBlock *flow.BlockHeader) {
	go a.bulkLoad(addressChan, currentBlock)
}

func (a *App) bulkLoad(addressChan chan []flow.Address, currentBlock *flow.BlockHeader) {
	start := time.Now()
	log.Info().Msg("Start Bulk Key Load")
	errLoad := a.dataLoader.RunAllAddressesLoader(addressChan, currentBlock)

	if errLoad != nil {
		log.Error().Err(errLoad).Msg("could not bulk load public keys")
	}
	duration := time.Since(start)
	log.Info().Msgf("End Bulk Load, duration %f min", duration.Minutes())
}

func (a *App) incrementalLoad(addressChan chan []flow.Address) {
	start := time.Now()
	pendingBlkHeight, _ := a.DB.GetPendingBlockHeight()

	log.Info().Msgf("Inc Load, %d start block \n", pendingBlkHeight)

	var blockHeight uint64
	var err error
	blockHeight, err = a.dataLoader.RunIncAddressesLoader(addressChan, pendingBlkHeight)
	if err != nil {
		log.Error().Err(err).Msg("could not load incremental public keys, retrying")
		var errRetry error
		blockHeight, errRetry = a.dataLoader.RunIncAddressesLoader(addressChan, pendingBlkHeight)
		if errRetry != nil {
			log.Error().Err(errRetry).Msg("could not load incremental public keys, retrying")
		}
	}
	duration := time.Since(start)

	if err == nil {
		a.DB.UpdatePendingBlockHeight(blockHeight)
	}

	log.Info().Msgf("Inc Load, %f sec, loading: %d (%d blockHeight)", duration.Seconds(), pendingBlkHeight, blockHeight)
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
