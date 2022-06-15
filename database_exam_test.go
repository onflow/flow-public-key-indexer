package main

import (
	"encoding/json"
	"testing"

	"github.com/rs/zerolog/log"
)

func TestExam(t *testing.T) {

	// test storing key and retreiving it
	db := NewDatabase("./db", false)

	items := db.ReadKeyValues(100)
	log.Info().Msgf("%v", len(items))

	for i := 0; i < len(items); i++ {
		key := items[i]
		keyValue, err := db.GetPublicKey(key, -1, -1, false, false)
		if err == nil {
			response, _ := json.Marshal(keyValue)
			log.Info().Msgf("%v", string(response))
		}
	}

}
