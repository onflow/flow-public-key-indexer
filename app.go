package main

import (
	"strings"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog/log"
)

type Params struct {
	LogLevel            string `default:"info"`
	Port                string `default:"8888"`
	FlowUrl             string `default:"access.mainnet.nodes.onflow.org:9000"`
	DbPath              string `default:"./db"`
	ChainId             string `default:"flow-mainnet"`
	MaxAcctKeys         int    `default:"500"`
	BatchSize           int    `default:"500"`
	IgnoreZeroWeight    bool   `default:"true"`
	IgnoreRevoked       bool   `default:"true"`
	ConcurrenClients    int    `default:"2"`
	WaitNumBlocks       int    `default:"500"`
	BlockPolIntervalSec int    `default:"120"`
	MaxBlockRange       int    `default:"1000"`
}
type App struct {
	DB         *Database
	flowClient *FlowAdapter
	p          Params
	dataLoader *DataLoader
	rest       *Rest
}

func (a *App) Initialize(params Params) {
	a.p = params
	a.DB = NewDatabase(params.DbPath)
	a.flowClient = NewFlowClient(strings.TrimSpace(a.p.FlowUrl))
	a.dataLoader = NewDataLoader(*a.DB, *a.flowClient, params)
	a.rest = NewRest(*a.DB, *a.flowClient, params)
}

func (a *App) Run() {

	go func() { a.loadPublicKeyData() }()

	a.rest.Start()
}

func (a *App) loadPublicKeyData() {
	addressChan := make(chan []flow.Address)
	a.dataLoader.SetupAddressLoader(addressChan)

	// note: get current block pass into incremetal
	// testing
	currentBlock, _ := a.flowClient.GetCurrentBlockHeight()
	a.DB.updateLoadingBlockHeight(currentBlock)
	updatedBlkHeight, _ := a.DB.GetUpdatedBlockHeight()

	// if restarted during initial loading, restart bulk load
	if updatedBlkHeight == 0 {
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
	// clear to get ready for bulk load
	a.DB.ClearAllData()
	start := time.Now()
	log.Info().Msg("Start Bulk Key Load")
	currentBlock, _ := a.flowClient.GetCurrentBlockHeight()
	// sets starting block height for incremental loader
	a.DB.updateLoadingBlockHeight(currentBlock - uint64(200))
	errLoad := a.dataLoader.RunAllAddressesLoader(addressChan)
	if errLoad != nil {
		log.Fatal().Err(errLoad).Msg("could not bulk load public keys")
	}
	// indicates bulk load has finished
	a.DB.updateBlockHeight(currentBlock)
	duration := time.Since(start)
	log.Info().Msgf("End Bulk Load, duration %f min", duration.Minutes())
}

func (a *App) increamentalLoad(addressChan chan []flow.Address, maxBlockRange int, waitNumBlocks int) {
	start := time.Now()

	loadingBlkHeight, _ := a.DB.GetLoadingBlockHeight()
	updatedToBlock, _ := a.DB.GetUpdatedBlockHeight()
	currentBlock, err := a.flowClient.GetCurrentBlockHeight()
	if err != nil {
		log.Warn().Err(err).Msg("could not get current block height from api")
	}

	// initial bulk loading is going on
	isLoading := updatedToBlock == 0

	loadingBlockRange := int(currentBlock) - int(loadingBlkHeight)
	isLoadingOutOfRange := loadingBlockRange >= maxBlockRange

	if isLoadingOutOfRange {
		if isLoading {
			log.Fatal().Msg("loading will not catch up, adjust run parameters to speed up load time")
			return
		}
		log.Warn().Msg("incremental loading will not catch up, starting bulk loading, if this happens again adjust running parameters")
		go func() { a.bulkLoad(addressChan) }()
		return
	}

	log.Debug().Msgf("check inc: (%t) %d (%d blks)", loadingBlockRange >= waitNumBlocks, currentBlock, loadingBlockRange)
	if loadingBlockRange <= waitNumBlocks {
		// need to wait for more blocks
		return
	}
	addressCount, restart := a.dataLoader.RunIncAddressesLoader(addressChan, isLoading, loadingBlkHeight)
	duration := time.Since(start)
	log.Info().Msgf("Inc Load, %f sec, (%d blk) curr: %d (%d addr)", duration.Seconds(), loadingBlockRange, currentBlock, addressCount)
	if restart && !isLoading {
		log.Info().Msg("Force restart bulk load")
		go func() { a.bulkLoad(addressChan) }()
	}
}
