package main

import (
	"context"

	"example/flow-key-indexer/model"
	"example/flow-key-indexer/pkg/pg"
	"fmt"

	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog/log"
)

func backfillPublicKeys(db *pg.Store, flowClient *FlowAdapter, params Params) error {
	ctx := context.Background()
	batchSize := 1000
	ignoreList := []string{}

	for {
		// Fetch a batch of unique addresses from the database
		log.Debug().Msgf("ignoreList: %v", ignoreList)
		addresses, err := db.GetUniqueAddressesWithoutAlgos(batchSize, ignoreList)
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

		log.Debug().Msgf("Processing %d addresses, %v", len(flowAddresses), flowAddresses)
		updatedRecords, err := ProcessAddressWithScript(ctx, params, flowAddresses, log.Logger, flowClient.Client, params.FetchSlowDownMs)

		ignoreList = extractAddresses(addresses, updatedRecords, ignoreList)

		if err != nil {
			log.Error().Err(err).Msg("Failed to process addresses")
			// Continue with the next batch instead of returning an error
			continue
		}

		// Update the database with the new information
		if err := db.InsertPublicKeyAccounts(ctx, updatedRecords); err != nil {
			log.Error().Err(err).Msg("Failed to update records")
			// Continue with the next batch instead of returning an error
			continue
		}

	}

	return nil
}

// return addresses that do not have an update record to be added to the ignore list
func extractAddresses(addresses []string, updateRecords []model.PublicKeyAccountIndexer, ignoreList []string) []string {
	// Use a map to make searching through updateRecords more efficient
	recordMap := make(map[string]bool)

	// Populate the map with the addresses from updateRecords
	for _, record := range updateRecords {
		recordMap[record.Account] = true
	}

	// Iterate through addresses and check if they exist in the recordMap
	for _, address := range addresses {
		if _, found := recordMap[address]; !found {
			ignoreList = append(ignoreList, address)
		}
	}

	return ignoreList
}
