package main

import (
	"context"
	"strings"

	"example/flow-key-indexer/model"
	"example/flow-key-indexer/pkg/pg"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/access"
	"github.com/rs/zerolog/log"
)

func backfillPublicKeys(ctx context.Context, flowAddresses []flow.Address, db *pg.Store, client access.Client, params Params) error {

	if len(flowAddresses) == 0 {
		log.Info().Msg("No more addresses to process. Backfill complete.")
		return nil
	}
	log.Info().Msgf("Batch Bulk Backfilling %v", len(flowAddresses))
	updatedRecords, err := ProcessAddressWithScript(ctx, params, flowAddresses, log.Logger, client, params.FetchSlowDownMs)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to process addresses, processing them individually")
		// loop over each address and process them individually
		for _, addr := range flowAddresses {
			updatedRecords, err := ProcessAddressWithScript(ctx, params, []flow.Address{addr}, log.Logger, client, params.FetchSlowDownMs)
			if err != nil {
				log.Error().Err(err).Msg("Failed to process address")
				continue
			}
			if len(updatedRecords) == 0 {
				log.Info().Msgf("No updated records for %v", addr)
				continue
			}
			log.Debug().Msgf("updatedRecords: %v", updatedRecords)
			_, err = generateAndSaveCopyString(ctx, db, updatedRecords)
			if err != nil {
				log.Error().Err(err).Msg("Failed to generate and save copy string")
				continue
			}
		}
		return nil

	} else {
		if len(updatedRecords) == 0 {
			log.Info().Msgf("No updated records to process, %v", flowAddresses)
			return nil
		}
		_, err := generateAndSaveCopyString(ctx, db, updatedRecords)
		if err != nil {
			log.Error().Err(err).Msg("Failed to generate and save copy string")
			return err
		}
	}

	return nil
}

func generateAndSaveCopyString(ctx context.Context, db *pg.Store, updatedRecords []model.PublicKeyAccountIndexer) (int64, error) {
	copyString, err := db.GenerateCopyStringForPublicKeyAccounts(ctx, updatedRecords)
	if err != nil {
		return 0, err
	}
	// Create an io.Reader from the copyString
	copyReader := strings.NewReader(copyString)

	log.Debug().Msgf("updatedRecords: %v", len(updatedRecords))

	rowsCopied, err := db.LoadPublicKeyIndexerFromReader(ctx, copyReader)
	if err != nil {
		return 0, err
	}
	return rowsCopied, nil
}
