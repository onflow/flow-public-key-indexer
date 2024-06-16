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
	"fmt"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/client"
	flowclient "github.com/onflow/flow-go-sdk/client"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// getFlowClient initializes and returns a flow client
func getFlowClient(flowClientUrl string) *flowclient.Client {
	flowClient, err := flowclient.New(flowClientUrl, grpc.WithInsecure())
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
	client *client.Client,
	highPriorityChan chan []flow.Address,
	lowPriorityChan chan []flow.Address,
	insertionHandler func(context.Context, []model.PublicKeyAccountIndexer) error,
	config Params,
) error {
	if client == nil {
		return fmt.Errorf("batch Failed to initialize flow client")
	}
	lowPriorityWorkerCount := 2
	bufferSize := 1000
	resultsChan := make(chan []model.PublicKeyAccountIndexer, bufferSize)
	fetchSlowdown := config.FetchSlowDownMs

	// Launch a goroutine to handle results
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("Batch Context done, exiting result handler")
				return
			case keys, ok := <-resultsChan:
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
				log.Info().Msg("Batch Context done, exiting high-priority worker")
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
	for i := 1; i <= lowPriorityWorkerCount; i++ {
		go func(workerID int) {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Msgf("Batch Low-priority worker %d recovered from panic: %v", workerID, r)
				}
			}()

			for {
				select {
				case <-ctx.Done():
					log.Info().Msgf("Batch Context done, exiting low-priority worker %d", workerID)
					return
				case accountAddresses, ok := <-lowPriorityChan:
					if !ok {
						log.Warn().Msgf("Batch Bulk Low-priority channel closed, worker %d exiting", workerID)
						return
					}
					if len(accountAddresses) == 0 {
						continue
					}
					start := time.Now()
					log.Debug().Msgf("Batch Bulk worker %d processing %d addresses, q(%d)", workerID, len(accountAddresses), len(lowPriorityChan))
					// second worker gets longer fetchSlowdown
					fetchSlowdown = fetchSlowdown * workerID
					currentBlock, err := client.GetLatestBlockHeader(ctx, true)
					if err != nil {
						log.Error().Err(err).Msg("Batch Bulk Could not get current block height from default flow client")
						return
					}
					accountKeys, err := ProcessAddressWithScript(ctx, config, accountAddresses, log, client, fetchSlowdown, currentBlock.Height)
					if err != nil {
						log.Error().Err(err).Msgf("Batch Bulk Failed Script Load, w(%d) addresses with script", workerID)
						return
					}
					duration := time.Since(start)
					log.Info().Msgf("Batch Bulk Finished Script Load, duration(%f) w(%d) %v, block %d, q(%d)", duration.Seconds(), workerID, len(accountKeys), currentBlock.Height, len(lowPriorityChan))
					if len(accountKeys) > 0 {
						resultsChan <- accountKeys
					}
				}
			}
		}(i)
	}

	return nil
}

func processAddresses(
	accountAddresses []flow.Address,
	ctx context.Context,
	log zerolog.Logger,
	client *flowclient.Client,
	resultsChan chan []model.PublicKeyAccountIndexer,
	fetchSlowdown int, insertHandler func(context.Context, []model.PublicKeyAccountIndexer) error) {

	var keys []model.PublicKeyAccountIndexer

	if len(accountAddresses) == 0 {
		return
	}

	log.Info().Msgf("Batch API Processing addresses: %v", len(accountAddresses))

	for _, addr := range accountAddresses {
		addrStr := add0xPrefix(addr.String())
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
				Account:   add0xPrefix(addrStr),
				Weight:    0,
				KeyId:     0,
			})
		} else {
			for _, key := range acct.Keys {
				// Clean up the public key, remove the 0x prefix
				keys = append(keys, model.PublicKeyAccountIndexer{
					PublicKey: strip0xPrefix(key.PublicKey.String()),
					Account:   add0xPrefix(addrStr),
					Weight:    key.Weight,
					KeyId:     key.Index,
				})
			}
		}
	}

	// Send the keys to the results channel
	err := insertHandler(ctx, keys)
	if err != nil {
		log.Error().Err(err).Msgf("Batch API Failed save keys, %v sending to DB channel instead", len(keys))
		resultsChan <- keys
	} else {
		log.Info().Msgf("Batch API Saved %v keys", len(keys))
	}

}
