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
				errHandler := insertionHandler(ctx, keys)
				if errHandler != nil {
					log.Error().Err(errHandler).Msg("Batch Failed to handle keys")
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
				go processAddresses(accountAddresses, ctx, log, client, resultsChan, fetchSlowdown)
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
					log.Debug().Msgf("Batch Bulk worker %d processing %d addresses, q(%d)", workerID, len(accountAddresses), len(lowPriorityChan))
					// second worker gets longer fetchSlowdown
					fetchSlowdown = fetchSlowdown * workerID
					currentBlock, err := client.GetLatestBlockHeader(ctx, true)
					if err != nil {
						log.Error().Err(err).Msg("Batch Bulk Could not get current block height from default flow client")
						return
					}
					log.Info().Msgf("Batch Bulk Start Script Load, w(%d) %v, block %d, q(%d)", workerID, len(accountAddresses), currentBlock.Height, len(lowPriorityChan))
					accountKeys, err := ProcessAddressWithScript(ctx, config, accountAddresses, log, client, fetchSlowdown, currentBlock.Height)
					if err != nil {
						log.Error().Err(err).Msgf("Batch Bulk Failed Script Load, w(%d) addresses with script", workerID)
						return
					}
					log.Info().Msgf("Batch Bulk Finished Script Load, w(%d) %v, block %d, q(%d)", workerID, len(accountKeys), currentBlock.Height, len(lowPriorityChan))
					resultsChan <- accountKeys
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
	fetchSlowdown int) {

	var keys []model.PublicKeyAccountIndexer
	var addrs []string
	// Convert flow.Address to string
	for _, addr := range accountAddresses {
		addrs = append(addrs, add0xPrefix(addr.String()))
	}

	if len(addrs) == 0 {
		return
	}

	// Convert filtered addresses back to flow.Address
	var validAddresses []flow.Address
	for _, addr := range addrs {
		validAddresses = append(validAddresses, flow.HexToAddress(addr))
	}

	log.Info().Msgf("Batch Processing addresses: %v", len(validAddresses))

	for _, addr := range validAddresses {
		addrStr := addr.String()
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
	resultsChan <- keys

}
