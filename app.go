package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
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
	PurgeReloadOnStart  bool   `default:"false"`
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
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func (a *App) Initialize(params Params) {
	d := Database{}
	a.p = params
	a.DB = d.Init(params.DbPath)
	a.flowClient = NewFlowClient(strings.TrimSpace(a.p.FlowUrl))
	a.dataLoader = NewDataLoader(d, params)

	a.loadKeyData()

	// a.saveMockData()
	// d.ReadValues()
}

func (a *App) Run() {
	// init router
	r := mux.NewRouter()
	r.HandleFunc("/keys/{id}", a.getKey).Methods("GET")
	r.HandleFunc("/keys-status", a.getStatus).Methods("GET")
	// handleRequests()
	log.Info().Msgf("Serving on PORT %s", a.p.Port)
	log.Fatal().Err(http.ListenAndServe(":"+a.p.Port, r)).Msg("Server at %s crashed!")

	log.Info().Msg("Here we are ")
}

func (a *App) getStatus(w http.ResponseWriter, r *http.Request) {
	block, err := a.flowClient.GetCurrentBlockHeight()
	stats := a.DB.Stats()
	stats.CurrentBlock = int(block)
	if err != nil {
		stats.CurrentBlock = -1
	}
	respondWithJSON(w, http.StatusOK, stats)
}

func (a *App) getKey(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r) // get params
	publicKey := params["id"]
	hashAlgo := r.URL.Query().Get("hashAlgo")
	hashAlgoTest, err := strconv.Atoi(hashAlgo)
	if err != nil {
		hashAlgoTest = -1
	}
	signAlgo := r.URL.Query().Get("signAlgo")
	signAlgoTest, err := strconv.Atoi(signAlgo)
	if err != nil {
		signAlgoTest = -1
	}
	// exclude zero weights
	exZero := r.URL.Query().Get("exZero")
	exZeroTest, err := strconv.ParseBool(exZero)
	if err != nil {
		exZeroTest = false
	}
	// exclude revoked keys
	exRevoked := r.URL.Query().Get("exRevoked")
	exRevokedTest, err := strconv.ParseBool(exRevoked)
	if err != nil {
		exRevokedTest = false
	}
	value, err := a.DB.GetPublicKey(publicKey, hashAlgoTest, signAlgoTest, exZeroTest, exRevokedTest)

	if err != nil {
		respondWithError(w, http.StatusNotFound, "")
		return
	}

	respondWithJSON(w, http.StatusOK, value)
}

func (a *App) loadKeyData() {

	// purge and reload key data
	if a.p.PurgeReloadOnStart {
		a.DB.ClearAllData()
		a.bulkLoad()
	}

	a.runIncrementalLoader(a.p.MaxBlockRange, a.p.WaitNumBlocks)
	// start ticker
	ticker := time.NewTicker(time.Duration(a.p.BlockPolIntervalSec) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				a.runIncrementalLoader(a.p.MaxBlockRange, a.p.WaitNumBlocks)
			case <-quit:
				log.Info().Msg("ticket is stopped")
				ticker.Stop()
				return
			}
		}
	}()
}

func (a *App) runIncrementalLoader(maxBlockRange, waitNumBlocks int) {
	addresses, forceRestart := a.increamentalLoad(maxBlockRange, waitNumBlocks)
	if forceRestart {
		a.DB.ClearAllData()
		a.bulkLoad()
	} else if len(addresses) > 0 {
		// load these addresses
		blockHeight, err := a.dataLoader.RunAddressLoader(addresses)
		if err != nil {
			log.Warn().Err(err).Msg("could not load addresses")
		}
		if err == nil {
			// only updated block height if no errors
			a.DB.updateBlockHeight(blockHeight)
		}
	}
}

func (a *App) bulkLoad() uint64 {
	start := time.Now()
	log.Info().Msg("Start Bulk Key Load")
	blockHeight, errLoad := a.dataLoader.RunBulkLoader()
	if errLoad != nil {
		log.Fatal().Err(errLoad).Msg("could not bulk load keys")
	}
	a.DB.updateBlockHeight(blockHeight)
	duration := time.Since(start)
	log.Info().Msgf("End Bulk Key Load, block height %d duration sec %f", blockHeight, duration.Seconds())
	return blockHeight
}

func (a *App) increamentalLoad(maxBlockRange int, waitNumBlocks int) ([]string, bool) {
	start := time.Now()
	lowerBlkHeight, errBh := a.DB.GetBlockHeight()
	if lowerBlkHeight == 0 {
		var addrs []string
		return addrs, true
	}
	if errBh != nil {
		log.Warn().Err(errBh).Msg("could not bulk load keys")
	}
	addresses, currentBlockHeight, restart := a.flowClient.GetAddressesFromBlockEvents(a.p.ConcurrenClients, lowerBlkHeight, maxBlockRange, waitNumBlocks)
	a.DB.updateBlockHeight(currentBlockHeight)
	duration := time.Since(start)
	log.Info().Msgf("Inc Load, %f sec, %d addrs, (%d blk) curr: %d", duration.Seconds(), len(addresses), currentBlockHeight-lowerBlkHeight, currentBlockHeight)
	return addresses, restart
}
