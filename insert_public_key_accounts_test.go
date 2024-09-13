package main

import (
	"context"
	"example/flow-key-indexer/pkg/pg"
	logger "log"
	"testing"

	"example/flow-key-indexer/model"

	"github.com/axiomzen/envconfig"
	"github.com/rs/zerolog/log"
)

func TestDuplicatedKeysInBatches(t *testing.T) {
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

	// Batch 1: Insert unique keys
	batch1 := []model.PublicKeyAccountIndexer{
		{
			Account:   "Account1",
			KeyId:     0,
			PublicKey: "publicKey1",
			Weight:    1000,
		},
		{
			Account:   "Account2",
			KeyId:     0,
			PublicKey: "publicKey2",
			Weight:    1000,
		},
		{
			Account:   "Account3",
			KeyId:     0,
			PublicKey: "publicKey3",
			Weight:    1000,
		},
	}

	// Batch 2: Insert duplicates with different SigAlgo and HashAlgo to trigger ON CONFLICT
	batch2 := []model.PublicKeyAccountIndexer{
		{
			Account:   "Account1",
			KeyId:     0,
			PublicKey: "publicKey1",
			SigAlgo:   1,
			HashAlgo:  1,
		},
		{
			Account:   "Account2",
			KeyId:     0,
			PublicKey: "publicKey2",
			SigAlgo:   1,
			HashAlgo:  1,
		},
	}

	ctx := context.Background()

	// Insert Batch 1
	err = db.InsertPublicKeyAccounts(ctx, batch1)
	if err != nil {
		t.Fatalf("Failed to insert batch 1 of public key accounts: %v", err)
	}

	// Insert Batch 2 (with duplicates)
	err = db.InsertPublicKeyAccounts(ctx, batch2)
	if err != nil {
		t.Fatalf("Failed to insert batch 2 of public key accounts: %v", err)
	}

	// Verify inserted data
	checkKey1, err := db.GetAccountsByPublicKey("publicKey1")
	if err != nil {
		t.Fatalf("Failed to get accounts for publicKey1: %v", err)
	}
	checkKey2, err := db.GetAccountsByPublicKey("publicKey2")
	if err != nil {
		t.Fatalf("Failed to get accounts for publicKey2: %v", err)
	}
	checkKey3, err := db.GetAccountsByPublicKey("publicKey3")
	if err != nil {
		t.Fatalf("Failed to get accounts for publicKey3: %v", err)
	}

	// Expected behavior:
	// - publicKey1 should have 1 account after conflict resolution
	// - publicKey2 should have 1 account after conflict resolution
	// - publicKey3 should have 1 account (no conflict)
	if len(checkKey1.Accounts) != 1 || len(checkKey2.Accounts) != 1 || len(checkKey3.Accounts) != 1 {
		t.Errorf("Unexpected number of accounts: publicKey1=%d, publicKey2=%d, publicKey3=%d",
			len(checkKey1.Accounts), len(checkKey2.Accounts), len(checkKey3.Accounts))
	}

	// Verify the conflict resolution updated sigalgo and hashalgo
	if checkKey1.Accounts[0].SigAlgo != 1 || checkKey2.Accounts[0].SigAlgo != 1 {
		t.Errorf("Expected sigalgo to be 1 after conflict resolution")
	}
}
