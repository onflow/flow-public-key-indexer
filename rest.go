package main

import (
	"encoding/json"
	"example/flow-key-indexer/pkg/pg"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

type Rest struct {
	DB         pg.Store
	flowClient FlowAdapter
	config     Params
}

func NewRest(DB pg.Store, fa FlowAdapter, p Params) *Rest {
	r := Rest{}
	r.DB = DB
	r.flowClient = fa
	r.config = p
	return &r
}

func (rest *Rest) Start() {
	// init router
	r := mux.NewRouter()
	r.HandleFunc("/key/{id}", rest.getKey).Methods("GET")
	r.HandleFunc("/key/{id}", rest.getKey).Methods("OPTIONS")
	r.HandleFunc("/status", rest.getStatus).Methods("GET")
	// handleRequests()
	log.Info().Msgf("Serving on PORT %s", rest.config.Port)
	log.Fatal().Err(http.ListenAndServe(":"+rest.config.Port, r)).Msg("Server at %s crashed!")
}

func (rest *Rest) getStatus(w http.ResponseWriter, r *http.Request) {
	block, err := rest.flowClient.GetCurrentBlockHeight()
	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
	}
	stats := rest.DB.Stats()
	stats.CurrentBlock = int(block)
	if err != nil {
		stats.CurrentBlock = -1
	}
	respondWithJSON(w, http.StatusOK, stats)
}

func (rest *Rest) getKey(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r) // get params
	publicKey := params["id"]
	key := strip0xPrefix(publicKey)
	value, err := rest.DB.GetAccountsByPublicKey(key)

	if err != nil {
		respondWithError(w, http.StatusNotFound, err.Error())
		return
	}
	respondWithJSON(w, http.StatusOK, value)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	allowedHeaders := "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization,X-CSRF-Token"
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(code)
	w.Write(response)
}
