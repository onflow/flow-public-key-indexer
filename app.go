package main

import (
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
	DbPath              string   `default:"./db"`
	ChainId             string   `default:"flow-mainnet"`
	MaxAcctKeys         int      `default:"1000"`
	BatchSize           int      `default:"100"`
	IgnoreZeroWeight    bool     `default:"true"`
	IgnoreRevoked       bool     `default:"true"`
	WaitNumBlocks       int      `default:"200"`
	BlockPolIntervalSec int      `default:"180"`
	MaxBlockRange       int      `default:"600"`
	FetchSlowDownMs     int      `default:"50"`
	SilenceBadgerdb     bool     `default:"true"`
	PurgeOnStart        bool     `default:"false"`
	EnableSyncData      bool     `default:"true"`

	PostgreSQLHost              string        `default:"localhost"`
	PostgreSQLPort              uint16        `default:"5432"`
	PostgreSQLUsername          string        `default:"postgres"`
	PostgreSQLPassword          string        `required:"false"`
	PostgreSQLDatabase          string        `required:"true"`
	PostgreSQLSSL               bool          `default:"true"`
	PostgreSQLLogQueries        bool          `default:"false"`
	PostgreSQLSetLogger         bool          `default:"false"`
	PostgreSQLRetryNumTimes     uint16        `default:"30"`
	PostgreSQLRetrySleepTime    time.Duration `default:"1s"`
	PostgreSQLPoolSize          int           `required:"true"`
	PostgresLoggerPrefix        string        `required:"true"`
	PostgresPrometheusSubSystem string        `required:"true"`
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
	// if anything happens close the db
	defer a.DB.Stop()
	if a.p.EnableSyncData {
		log.Info().Msgf("Data Sync service is enabled")
		go func() { a.loadPublicKeyData() }()
	}

	a.rest.Start()
}

func (a *App) loadPublicKeyData() {
	addressChan := make(chan []flow.Address)
	a.dataLoader.SetupAddressLoader(addressChan)

	// note: get current block pass into incremetal
	// testing
	currentBlock, err := a.flowClient.GetCurrentBlockHeight()
	log.Debug().Msgf("Current block from server %v", currentBlock)
	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
	}
	a.DB.UpdateLoadingBlockHeight(currentBlock)
	updatedBlkHeight, _ := a.DB.GetUpdatedBlockHeight()
	isTooFarBehind := currentBlock-updatedBlkHeight > uint64(a.p.MaxBlockRange)

	// if restarted during initial loading, restart bulk load
	if updatedBlkHeight == 0 || isTooFarBehind {
		if isTooFarBehind && updatedBlkHeight != 0 {
			log.Info().Msg("Incremental is too far behind, starting bulk load")
		}
		go func() { a.bulkLoad(addressChan) }()
	}

	// start ticker
	ticker := time.NewTicker(time.Duration(a.p.BlockPolIntervalSec) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				go func() {
					a.increamentalLoad(addressChan, a.p.MaxBlockRange, a.p.WaitNumBlocks)
				}()
			case <-quit:
				log.Info().Msg("ticket is stopped")
				ticker.Stop()
				return
			}
		}
	}()

}

func (a *App) bulkLoad(addressChan chan []flow.Address) {
	start := time.Now()
	log.Info().Msg("Start Bulk Key Load")
	currentBlock, err := a.flowClient.GetCurrentBlockHeight()
	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
	}
	// sets starting block height for incremental loader
	a.DB.UpdateLoadingBlockHeight(currentBlock)

	errLoad := a.dataLoader.RunAllAddressesLoader(addressChan)
	//errLoad := testRunAddresses(addressChan)

	if errLoad != nil {
		log.Error().Err(errLoad).Msg("could not bulk load public keys")
	}
	duration := time.Since(start)
	log.Info().Msgf("End Bulk Load, duration %f min", duration.Minutes())
}

/*
func testRunAddresses(addressChan chan []flow.Address) error {
	// mainnet
	addresses := []flow.Address{flow.HexToAddress("0xb643d57edb1740c6"), flow.HexToAddress("0x1b1c31af469bc4dc")}
	// testnet
	//addresses := []flow.Address{flow.HexToAddress("0x668b91e2995c2eba"), flow.HexToAddress("0x1d83294670972f97")}
	addressChan <- addresses
	return nil
}
*/
func (a *App) increamentalLoad(addressChan chan []flow.Address, maxBlockRange int, waitNumBlocks int) {
	start := time.Now()
	loadingBlkHeight, _ := a.DB.GetLoadingBlockHeight()
	updatedToBlock, _ := a.DB.GetUpdatedBlockHeight()
	currentBlock, err := a.flowClient.GetCurrentBlockHeight()
	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
	}
	// initial bulk loading is going on
	isLoading := updatedToBlock == 0

	loadingBlockRange := int(currentBlock) - int(loadingBlkHeight)
	isLoadingOutOfRange := loadingBlockRange >= maxBlockRange

	if isLoadingOutOfRange {
		log.Debug().Msgf("curr: %d ldg blk: %d diff: %d max: %d", currentBlock, loadingBlkHeight, loadingBlockRange, maxBlockRange)
		log.Error().Msg("loading will not catch up, adjust run parameters to speed up load time")
		return
	}

	log.Debug().Msgf("check inc: (%t) (curr: %d) (%d blks)", loadingBlockRange >= waitNumBlocks, currentBlock, loadingBlockRange)
	if loadingBlockRange <= waitNumBlocks {
		// need to wait for more blocks
		return
	}
	// start loading from next block
	startLoadingFrom := loadingBlkHeight + 1
	addressCount, restart := a.dataLoader.RunIncAddressesLoader(addressChan, isLoading, startLoadingFrom)
	duration := time.Since(start)
	log.Info().Msgf("Inc Load, %f sec, (%d blk) curr: %d (%d addr)", duration.Seconds(), loadingBlockRange, currentBlock, addressCount)

	if restart && !isLoading {
		log.Warn().Msg("Force restart bulk load")
		go func() { a.bulkLoad(addressChan) }()
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

func getPostgresConfig(conf Params, logger zerolog.Logger) pg.Config {
	return pg.Config{
		ConnectPGOptions: pg.ConnectPGOptions{
			ConnectionString: getPostgresConnectionString(conf),
			RetrySleepTime:   conf.PostgreSQLRetrySleepTime,
			RetryNumTimes:    conf.PostgreSQLRetryNumTimes,
			TLSConfig:        nil,
			ConnErrorLogger: func(
				numTries int,
				duration time.Duration,
				host string,
				db string,
				user string,
				ssl bool,
				err error,
			) {
				// warn is probably a little strong here
				logger.Info().
					Int("numTries", numTries).
					Dur("duration", duration).
					Str("host", host).
					Str("db", db).
					Str("user", user).
					Bool("ssl", ssl).
					Err(err).
					Msg("connection failed")
			},
		},
		PGApplicationName: "public-key-indexer",
		PGLoggerPrefix:    conf.PostgresLoggerPrefix,
		PGPoolSize:        conf.PostgreSQLPoolSize,
	}
}

func getPostgresConnectionString(conf Params) string {
	return pg.ToURL(
		int(conf.PostgreSQLPort),
		conf.PostgreSQLSSL,
		conf.PostgreSQLUsername,
		conf.PostgreSQLPassword,
		conf.PostgreSQLDatabase,
		conf.PostgreSQLHost,
	)
}
