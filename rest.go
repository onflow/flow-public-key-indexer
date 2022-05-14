package main

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

type Rest struct {
	DB         Database
	flowClient FlowAdapter
	config     Params
}

func NewRest(DB Database, fa FlowAdapter, p Params) *Rest {
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
	r.HandleFunc("/status", rest.getStatus).Methods("GET")
	// handleRequests()
	log.Info().Msgf("Serving on PORT %s", rest.config.Port)
	log.Fatal().Err(http.ListenAndServe(":"+rest.config.Port, r)).Msg("Server at %s crashed!")
}

func (rest *Rest) getStatus(w http.ResponseWriter, r *http.Request) {
	block, err := rest.flowClient.GetCurrentBlockHeight()
	stats := rest.DB.Stats()
	stats.CurrentBlock = int(block)
	if err != nil {
		stats.CurrentBlock = -1
	}
	respondWithJSON(w, http.StatusOK, stats)
}

func (rest *Rest) getKey(w http.ResponseWriter, r *http.Request) {
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
	value, err := rest.DB.GetPublicKey(publicKey, hashAlgoTest, signAlgoTest, exZeroTest, exRevokedTest)

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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}
