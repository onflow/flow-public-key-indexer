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

var DefaultConfig = Config{
	BatchSize:          100,
	AtBlockHeight:      0,
	FlowAccessNodeURLs: []string{"access.mainnet.nodes.onflow.org:9000"},
	Pause:              0,
	ChainID:            "flow-mainnet",
}

// Ignore list for accounts that keep getting same public keys added
var ignoreAccounts = map[string]bool{
	"0xbf48a20670f179b8": true, // placeholder, replace when account identified
}

// addresses that error and need reprocessing
var errorAddresses = map[string]bool{}

func ProcessAddressChannel(
	ctx context.Context,
	log zerolog.Logger,
	client *flowclient.Client,
	addressChan chan []flow.Address,
	handler func([]model.PublicKeyAccountIndexer) error,
	filter func([]string) ([]string, error),
) error {
	if client == nil {
		return fmt.Errorf("failed to initialize flow client")
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Msgf("Recovered from panic: %v", r)
			}
		}()

		// A separate channel to collect results from goroutines
		resultsChan := make(chan []model.PublicKeyAccountIndexer)

		// Launch a goroutine to handle results
		go func() {
			for {
				select {
				case <-ctx.Done():
					log.Info().Msg("Context done, exiting result handler")
					return
				case keys, ok := <-resultsChan:
					if !ok {
						log.Info().Msg("Results channel closed, exiting result handler")
						return
					}

					errHandler := handler(keys)
					if errHandler != nil {
						log.Error().Err(errHandler).Msg("Failed to handle keys")
					}
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("Context done, exiting ProcessAddressChannel")
				close(resultsChan)
				return
			case accountAddresses, ok := <-addressChan:
				if !ok {
					log.Info().Msg("Address channel closed, exiting ProcessAddressChannel")
					close(resultsChan)
					return
				}

				// Process the addresses concurrently
				go processAddresses(accountAddresses, ctx, log, client, resultsChan)
			}
		}
	}()

	return nil
}

func processAddresses(
	accountAddresses []flow.Address,
	ctx context.Context,
	log zerolog.Logger,
	client *flowclient.Client,
	resultsChan chan []model.PublicKeyAccountIndexer) {

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

	log.Info().Msgf("Batch: Processing addresses: %v", len(validAddresses))

	for _, addr := range validAddresses {
		addrStr := addr.String()
		if _, ok := ignoreAccounts[addrStr]; ok {
			continue
		}

		time.Sleep(100 * time.Millisecond)

		acct, err := client.GetAccount(ctx, addr)
		if err != nil {
			log.Warn().Err(err).Msgf("Batch: Failed to get account, %v", addrStr)
			errorAddresses[addrStr] = true
			continue
		}
		if acct == nil {
			log.Warn().Msgf("Batch: Account not found: %v", addrStr)
			continue
		}
		if acct.Keys == nil {
			log.Warn().Msgf("Batch: Account has nil Keys: %v", addrStr)
			continue
		}
		if len(acct.Keys) == 0 {
			log.Warn().Msgf("Batch: Account has no keys: %v", addrStr)
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
