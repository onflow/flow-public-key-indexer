package main

import (
	"context"

	"example/flow-key-indexer/model"
	"example/flow-key-indexer/pkg/pg"
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
	if err := backfillPublicKeys(db, flowClient, params); err != nil {
		log.Fatal().Err(err).Msg("Failed to backfill public keys")
	}

	log.Info().Msg("Backfill process completed successfully")
}

func backfillPublicKeys(db *pg.Store, flowClient *FlowAdapter, params Params) error {
	batchSize := 1000

	for {
		// Fetch a batch of unique addresses from the database
		addresses, err := db.GetUniqueAddressesWithoutAlgos(batchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch addresses: %w", err)
		}

		if len(addresses) == 0 {
			log.Info().Msg("No more addresses to process. Backfill complete.")
			break
		}

		// Convert string addresses to flow.Address
		flowAddresses := make([]flow.Address, len(addresses))
		for i, addr := range addresses {
			flowAddresses[i] = flow.HexToAddress(addr)
		}

		// Process the batch
		updatedRecords, err := processRecords(flowAddresses, flowClient, params)
		if err != nil {
			log.Error().Err(err).Msg("Failed to process addresses")
			// Continue with the next batch instead of returning an error
			continue
		}

		// Update the database with the new information
		if err := db.UpdatePublicKeyAccounts(updatedRecords); err != nil {
			log.Error().Err(err).Msg("Failed to update records")
			// Continue with the next batch instead of returning an error
			continue
		}

		log.Info().Msgf("Processed and updated records for %d addresses", len(addresses))

	}

	return nil
}

func processRecords(addresses []flow.Address, flowClient *FlowAdapter, config Params) ([]model.PublicKeyAccountIndexer, error) {
	ctx := context.Background()
	currentBlock, err := flowClient.Client.GetLatestBlockHeader(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block header: %w", err)
	}

	publicKeys, err := ProcessAddressWithScript(ctx, config, addresses, log.Logger, flowClient.Client, config.FetchSlowDownMs, currentBlock.Height)
	return publicKeys, err
}

func removeDuplicates(slice []flow.Address) []flow.Address {
	keys := make(map[flow.Address]bool)
	list := []flow.Address{}
	for _, entry := range slice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
