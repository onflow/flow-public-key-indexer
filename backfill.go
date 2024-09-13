package main

import (
	"context"

	"example/flow-key-indexer/pkg/pg"
	"fmt"

	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog/log"
)

func backfillPublicKeys(db *pg.Store, flowClient *FlowAdapter, params Params) error {
	ctx := context.Background()
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

		updatedRecords, err := ProcessAddressWithScript(ctx, params, flowAddresses, log.Logger, flowClient.Client, params.FetchSlowDownMs)

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
