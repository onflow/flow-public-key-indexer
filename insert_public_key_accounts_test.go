package main

import (
	"example/flow-key-indexer/pkg/pg"
	"fmt"
	logger "log"
	"testing"

	"example/flow-key-indexer/model"

	"github.com/axiomzen/envconfig"
	"github.com/rs/zerolog/log"
)

// TestAdd tests the Add function.
func TestAdd(t *testing.T) {
	var p Params
	err := envconfig.Process("KEYIDX", &p)
	if err != nil {
		logger.Fatal(err.Error())
	}

	dbConfig := pg.DatabaseConfig{
		Host:     p.PostgreSQLHost,
		Password: p.PostgreSQLPassword,
		Name:     p.PostgreSQLDatabase,
		User:     p.PostgreSQLUsername,
		Port:     int(p.PostgreSQLPort),
	}
	db := pg.NewStore(dbConfig, log.Logger)
	_ = db.Start(true)

	key1 := model.PublicKeyAccountIndexer{
		Account:   "Account1",
		KeyId:     0,
		PublicKey: "publicKey1",
		Weight:    1000,
	}

	key2 := model.PublicKeyAccountIndexer{
		Account:   "Account2",
		KeyId:     0,
		PublicKey: "publicKey2",
		Weight:    1000,
	}

	key1Dup := model.PublicKeyAccountIndexer{
		Account:   "Account1",
		KeyId:     0,
		PublicKey: "publicKey1",
		Weight:    1000,
	}

	key3 := model.PublicKeyAccountIndexer{
		Account:   "Account3",
		KeyId:     0,
		PublicKey: "publicKey3",
		Weight:    1000,
	}
	key4 := model.PublicKeyAccountIndexer{
		Account:   "Account1",
		KeyId:     0,
		PublicKey: "publicKey2",
		Weight:    1000,
	}

	accountKeys := []model.PublicKeyAccountIndexer{}
	accountKeys = append(accountKeys, key1, key2, key1Dup, key3, key4)
	db.InsertPublicKeyAccounts(accountKeys)

	checkKey1, _ := db.GetAccountsByPublicKey("publicKey1")
	checkKey2, _ := db.GetAccountsByPublicKey("publicKey2")
	checkKey3, _ := db.GetAccountsByPublicKey("publicKey3")
	fmt.Println(checkKey1)
	fmt.Println(checkKey2)
	fmt.Println(checkKey3)

	if len(checkKey1.Accounts) != 1 || len(checkKey2.Accounts) != 2 || len(checkKey3.Accounts) != 1 {
		t.Errorf("Error inserting public keys")
	}
}
