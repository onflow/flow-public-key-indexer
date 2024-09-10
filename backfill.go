package main

import (
	"context"

	"example/flow-key-indexer/model"
	"example/flow-key-indexer/pkg/pg"
	"example/flow-key-indexer/utils"
	"fmt"
	logger "log"

	"github.com/axiomzen/envconfig"
	"github.com/joho/godotenv"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func backfill() {

	if err := godotenv.Load("./.env"); err != nil {
		logger.Fatal("error loading godotenv")
	}
	var params Params
	err := envconfig.Process("KEYIDX", &params)

	if err != nil {
		logger.Fatal(err.Error())
	}
	lvl, err := zerolog.ParseLevel(params.LogLevel)
	if err == nil {
		zerolog.SetGlobalLevel(lvl)
		log.Info().Msgf("Set log level to %s", lvl.String())
	}

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Initialize database connection
	dbConfig := getPostgresConfig(params, log.Logger)
	db := pg.NewStore(dbConfig, log.Logger)
	err = db.Start(false)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to database")
	}
	//TODO: figure out how to close
	//	defer store.Close()

	// Initialize Flow client
	flowClient := NewFlowClient(params.FlowUrl1)

	// Run the backfill process
	if err := backfillPublicKeys(db, flowClient); err != nil {
		log.Fatal().Err(err).Msg("Failed to backfill public keys")
	}

	log.Info().Msg("Backfill process completed successfully")
}

func backfillPublicKeys(db *pg.Store, flowClient *FlowAdapter) error {
	var offset int64 = 0
	for {
		// Fetch a batch of records from the database
		records, err := db.GetPublicKeyAccountsWithoutAlgos(1000, offset)
		if err != nil {
			return fmt.Errorf("failed to fetch records: %w", err)
		}

		if len(records) == 0 {
			break // No more records to process
		}

		// Process the batch
		updatedRecords, err := processRecords(records, flowClient)
		if err != nil {
			return fmt.Errorf("failed to process records: %w", err)
		}

		// Update the database with the new information
		if err := db.UpdatePublicKeyAccounts(updatedRecords); err != nil {
			return fmt.Errorf("failed to update records: %w", err)
		}

		log.Info().Msgf("Processed and updated %d records", len(updatedRecords))

		offset += int64(len(records))
	}

	return nil
}

func processRecords(records []model.PublicKeyAccountIndexer, flowClient *FlowAdapter) ([]model.PublicKeyAccountIndexer, error) {
	ctx := context.Background()
	updatedRecords := make([]model.PublicKeyAccountIndexer, 0, len(records))

	for _, record := range records {
		address := flow.HexToAddress(utils.Strip0xPrefix(record.Account))
		account, err := flowClient.Client.GetAccount(ctx, address)
		if err != nil {
			log.Warn().Err(err).Str("address", record.Account).Msg("Failed to fetch account")
			continue
		}

		for _, key := range account.Keys {
			if int(key.Index) == record.KeyId {
				record.SigAlgo = int(key.SigAlgo)
				record.HashAlgo = int(key.HashAlgo)
				updatedRecords = append(updatedRecords, record)
				break
			}
		}
	}

	return updatedRecords, nil
}
