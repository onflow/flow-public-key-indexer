package main

import (
	"example/flow-key-indexer/pkg/pg"
	"fmt"
	"testing"

	"example/flow-key-indexer/model"

	"github.com/rs/zerolog/log"
)

// TestAdd tests the Add function.
func TestAdd(t *testing.T) {
	dbConfig := pg.DatabaseConfig{
		Host:     "localhost",
		Password: "password",
		Name:     "key-indexer",
		User:     "postgres",
		Port:     5432,
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
