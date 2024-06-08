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

func GetAllAddresses(
	ctx context.Context,
	log zerolog.Logger,
	conf Config,
	addressChan chan []flow.Address,
	currentBlock *flow.BlockHeader,
	validateAddress func(string) bool,
) (height uint64, err error) {
	flowClient := getFlowClient(conf.FlowAccessNodeURLs[0])

	ap, err := InitAddressProvider(ctx, log, conf.ChainID, currentBlock.ID, flowClient, conf.Pause, validateAddress)
	if err != nil {
		return 0, err
	}
	ap.GenerateAddressBatches(addressChan, conf.BatchSize)

	return currentBlock.Height, nil
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
				go func(accountAddresses []flow.Address) {
					var keys []model.PublicKeyAccountIndexer
					log.Debug().Msgf("Validating %d addresses", len(accountAddresses))
					var addrs []string
					// Convert flow.Address to string
					for _, addr := range accountAddresses {
						addrs = append(addrs, add0xPrefix(addr.String()))
					}
					filteredAddresses, err := filter(addrs)
					if err != nil {
						log.Error().Err(err).Msg("Failed to filter addresses")
						return
					}

					log.Debug().Msgf("Processing addresses: %v", len(filteredAddresses))
					if len(filteredAddresses) == 0 {
						return
					}

					// Convert filtered addresses back to flow.Address
					var validAddresses []flow.Address
					for _, addr := range filteredAddresses {
						validAddresses = append(validAddresses, flow.HexToAddress(addr))
					}

					for _, addr := range validAddresses {
						addrStr := addr.String()
						if _, ok := ignoreAccounts[addrStr]; ok {
							continue
						}

						acct, err := client.GetAccount(ctx, addr)
						if err != nil {
							log.Error().Err(err).Msg("Failed to get account")
							errorAddresses[addrStr] = true
							continue
						}
						if acct == nil {
							log.Debug().Msgf("Account not found: %v", addrStr)
							continue
						}
						if acct.Keys == nil {
							log.Debug().Msgf("Account has nil Keys: %v", addrStr)
							continue
						}
						if len(acct.Keys) == 0 {
							log.Debug().Msgf("Account has no keys: %v", addrStr)
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

					for eAddr := range errorAddresses {
						log.Warn().Msgf("addressChan: Process again address to error list: %v", eAddr)
						addressChan <- []flow.Address{flow.HexToAddress(eAddr)}
					}
				}(accountAddresses)
			}
		}
	}()

	return nil
}
