/*
 * Cadence - The resource-oriented smart contract programming language
 *
 * Copyright 2022 Dapper Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	_ "embed"
	"example/flow-key-indexer/model"
	"example/flow-key-indexer/pkg/pg"
	"example/flow-key-indexer/utils"
	"fmt"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/access"
	"github.com/onflow/flow-go-sdk/access/grpc"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	grpcOpts "google.golang.org/grpc"
)

// getFlowClient initializes and returns a flow client
func getFlowClient(flowClientUrl string) *grpc.BaseClient {
	flowClient, err := grpc.NewBaseClient(flowClientUrl,
		grpcOpts.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}

	return flowClient
}

// Config defines that application's config
type Config struct {
	// BachSize is the number of addresses for which to run each script
	BatchSize int
	// 0 is treated as the latest block height.
	AtBlockHeight      uint64
	FlowAccessNodeURLs []string
	Pause              time.Duration
	ChainID            flow.ChainID
	maxAcctKeys        int
	ignoreZeroWeight   bool
	ignoreRevoked      bool
}

// Ignore list for accounts that keep getting same public keys added
var ignoreAccounts = map[string]bool{
	"0000000000000000": true, // placeholder, replace when account identified
}

func ProcessAddressChannels(
	ctx context.Context,
	log zerolog.Logger,
	client access.Client,
	highPriorityChan chan []flow.Address,
	lowPriorityChan chan []flow.Address,
	db *pg.Store,
	config Params,
) error {
	if client == nil {
		return fmt.Errorf("batch Failed to initialize flow client")
	}
	bufferSize := 1000
	resultsChan := make(chan []model.PublicKeyAccountIndexer, bufferSize)
	fetchSlowdown := config.FetchSlowDownMs

	insertionHandler := db.InsertPublicKeyAccounts
	// Launch a goroutine to handle results
	go func() {
		log.Debug().Msg("Batch Results channel handler started")
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("Batch Context done, exiting result handler")
				return
			case keys, ok := <-resultsChan:
				log.Debug().Msgf("Batch Results channel received %v keys", len(keys))
				if !ok {
					log.Info().Msg("Batch Results channel closed, exiting result handler")
					return
				}

				if len(keys) == 0 {
					continue
				}

				start := time.Now()
				errHandler := insertionHandler(ctx, keys)
				duration := time.Since(start)
				log.Info().Msgf("Batch DB d(%f) q(%v) to be stored", duration.Seconds(), len(resultsChan))
				if errHandler != nil {
					log.Error().Err(errHandler).Msgf("Batch Failed to handle keys, %v, break up into smaller chunks", len(keys))
					// break up batch into smaller chunks
					for i := 0; i < len(keys); i += config.BatchSize {
						end := i + config.BatchSize
						if end > len(keys) {
							end = len(keys)
						}
						errHandler = insertionHandler(ctx, keys[i:end])
						if errHandler != nil {
							log.Error().Err(errHandler).Msg("Batch Failed to handle keys")
						}
					}
				}
			}
		}
	}()

	// High-priority worker
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Msgf("Batch High-priority worker recovered from panic: %v", r)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("Batch High-priority Context done, exiting high-priority worker")
				return
			case accountAddresses, ok := <-highPriorityChan:
				if !ok {
					log.Warn().Msg("Batch High-priority channel closed, exiting high-priority worker")
					return
				}
				// Create a new goroutine to process each high-priority address array
				log.Debug().Msgf("Batch High-priority worker processing %d addresses", len(accountAddresses))
				go processAddresses(accountAddresses, ctx, log, client, resultsChan, fetchSlowdown, insertionHandler)
			}
		}
	}()

	// Low-priority workers
	go func() {
		log.Info().Msgf("Batch Bulk Low-priority started")
		defer func() {
			if r := recover(); r != nil {
				log.Error().Msgf("Batch Low-priority worker recovered from panic: %v", r)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				log.Info().Msgf("Batch Bulk Context done, exiting low-priority")
				return
			case accountAddresses, ok := <-lowPriorityChan:
				if !ok {
					log.Warn().Msgf("Batch Bulk Low-priority channel closed, exiting")
					return
				}
				if len(accountAddresses) == 0 {
					continue
				}
				log.Debug().Msgf("Batch Bulk Low-priority processing %d addresses", len(accountAddresses))
				err := backfillPublicKeys(ctx, accountAddresses, db, client, config)
				if err != nil {
					log.Error().Err(err).Msgf("Batch Bulk Low-priority failed to backfill addresses %d", len(accountAddresses))
				}
			}
		}
	}()

	return nil
}

func processAddresses(
	accountAddresses []flow.Address,
	ctx context.Context,
	log zerolog.Logger,
	client access.Client,
	resultsChan chan []model.PublicKeyAccountIndexer,
	fetchSlowdown int, insertHandler func(context.Context, []model.PublicKeyAccountIndexer) error) {

	var keys []model.PublicKeyAccountIndexer

	if len(accountAddresses) == 0 {
		return
	}

	log.Info().Msgf("Batch API Processing addresses: %v", len(accountAddresses))

	for _, addr := range accountAddresses {
		addrStr := utils.Add0xPrefix(addr.String())
		if _, ok := ignoreAccounts[addrStr]; ok {
			continue
		}

		time.Sleep(time.Duration(fetchSlowdown) * time.Millisecond)

		log.Debug().Msgf("Batch Getting account: %v", addrStr)
		acct, err := client.GetAccount(ctx, addr)
		log.Debug().Msgf("Batch Got account: %v", addrStr)

		if err != nil {
			log.Warn().Err(err).Msgf("Batch Failed to get account, %v", addrStr)
			continue
		}
		if acct == nil {
			log.Warn().Msgf("Batch Account not found: %v", addrStr)
			continue
		}
		if acct.Keys == nil {
			log.Warn().Msgf("Batch Account has nil Keys: %v", addrStr)
			continue
		}
		if len(acct.Keys) == 0 {
			log.Warn().Msgf("Batch Account has no keys: %v", addrStr)
			// Save account with blank public key to avoid querying it again
			keys = append(keys, model.PublicKeyAccountIndexer{
				PublicKey: "blank",
				Account:   utils.Add0xPrefix(addrStr),
				Weight:    0,
				KeyId:     0,
				IsRevoked: false,
			})
		} else {
			for _, key := range acct.Keys {
				// Clean up the public key, remove the 0x prefix
				keys = append(keys, model.PublicKeyAccountIndexer{
					PublicKey: utils.Strip0xPrefix(key.PublicKey.String()),
					Account:   utils.Add0xPrefix(addrStr),
					Weight:    key.Weight,
					KeyId:     int(key.Index),
					IsRevoked: key.Revoked,
					SigAlgo:   GetSignatureAlgoIndex(key.SigAlgo.String()),
					HashAlgo:  GetHashingAlgoIndex(key.HashAlgo.String()),
				})
			}
		}
	}

	log.Debug().Msgf("adding public keys: %v", keys)
	log.Debug().Msgf("Batch API Processed %v keys of %v addresses", len(keys), len(accountAddresses))
	// Send the keys to the results channel
	err := insertHandler(ctx, keys)
	if err != nil {
		log.Error().Err(err).Msgf("Batch API Failed save keys, %v sending to DB channel instead", len(keys))
		resultsChan <- keys
	} else {
		log.Info().Msgf("Batch API Saved %v keys of %v addresses", len(keys), len(accountAddresses))
	}

}

func GetHashingAlgoIndex(hashAlgo string) int {
	switch hashAlgo {
	case "SHA2_256":
		return 1
	case "SHA2_384":
		return 2
	case "SHA3_256":
		return 3
	case "SHA3_384":
		return 4
	case "KMAC128_BLS_BLS12_381":
		return 5
	case "KECCAK_256":
		return 6
	default:
		log.Warn().Msgf("Batch Unknown hashing algorithm: %v", hashAlgo)
		return 0 // Unknown
	}
}

func GetSignatureAlgoIndex(sigAlgo string) int {
	switch sigAlgo {
	case "ECDSA_P256":
		return 1
	case "ECDSA_secp256k1":
		return 2
	case "BLS_BLS12_381":
		return 3
	default:
		log.Warn().Msgf("Batch Unknown signature algorithm: %v", sigAlgo)
		return 0
	}
}
