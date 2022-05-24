package main

import (
	"strings"
	"time"

	"github.com/onflow/flow-go-sdk"
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
}
type App struct {
	DB         *Database
	flowClient *FlowAdapter
	p          Params
	dataLoader *DataLoader
	rest       *Rest
}

func (a *App) Initialize(params Params) {
	params.AllFlowUrls = setAllFlowUrls(params)
	a.p = params
	a.DB = NewDatabase(params.DbPath, params.SilenceBadgerdb)
	a.flowClient = NewFlowClient(strings.TrimSpace(a.p.FlowUrl1))
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
	isTooFarBehind := currentBlock-updatedBlkHeight > uint64(a.p.MaxBlockRange)

	// if restarted during initial loading, restart bulk load
	if updatedBlkHeight == 0 || isTooFarBehind {
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
	currentBlock, _ := a.flowClient.GetCurrentBlockHeight()
	// sets starting block height for incremental loader
	a.DB.updateLoadingBlockHeight(currentBlock)
	errLoad := a.dataLoader.RunAllAddressesLoader(addressChan)
	if errLoad != nil {
		log.Fatal().Err(errLoad).Msg("could not bulk load public keys")
	}
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
		log.Debug().Msgf("curr: %d ldg blk: %d diff: %d max: %d", currentBlock, loadingBlkHeight, loadingBlockRange, maxBlockRange)
		log.Fatal().Msg("loading will not catch up, adjust run parameters to speed up load time")
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
	if addressCount > 0 {
		go func() { a.DB.UpdateTotalPublicKeyCount() }()
	}
	if restart && !isLoading {
		log.Info().Msg("Force restart bulk load")
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
