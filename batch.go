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
	pauseInterval int,
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

		for accountAddresses := range addressChan {
			// Skip address if known broken
			var keys []model.PublicKeyAccountIndexer
			log.Debug().Msgf("Validating %d addresses", len(accountAddresses))
			var addrs []string
			// convert flow.Address to string
			for _, addr := range accountAddresses {
				addrs = append(addrs, add0xPrefix(addr.String()))
			}
			accountAddresses, err := filter(addrs)
			if err != nil {
				log.Error().Err(err).Msg("Failed to filter addresses")
				continue
			}

			log.Debug().Msgf("Processing addresses: %v", len(accountAddresses))
			if len(accountAddresses) == 0 {
				continue
			}

			for _, addr := range accountAddresses {
				if _, ok := ignoreAccounts[addr]; ok {
					continue
				}
				log.Debug().Msgf("pausing before getting address: %v", pauseInterval)
				time.Sleep(time.Duration(pauseInterval) * time.Millisecond)
				acct, err := client.GetAccount(ctx, flow.HexToAddress(addr))
				if err != nil {
					log.Error().Err(err).Msg("Failed to get account")
					continue
				}
				if acct == nil {
					log.Debug().Msgf("Account not found: %v", addr)
					continue
				}
				if acct.Keys == nil {
					log.Debug().Msgf("Account has nil Keys: %v", addr)
					continue
				}
				if len(acct.Keys) == 0 {
					log.Debug().Msgf("Account has no keys: %v", addr)
					// save account with blank public key to avoid querying it again
					keys = append(keys, model.PublicKeyAccountIndexer{
						PublicKey: "blank",
						Account:   add0xPrefix(addr),
						Weight:    0,
						KeyId:     0,
					})
				} else {
					for _, key := range acct.Keys {
						// clean up the public key, remove the 0x prefix
						keys = append(keys, model.PublicKeyAccountIndexer{
							PublicKey: strip0xPrefix(key.PublicKey.String()),
							Account:   add0xPrefix(addr),
							Weight:    key.Weight,
							KeyId:     key.Index,
						})
					}
				}
			}

			errHandler := handler(keys)
			if errHandler != nil {
				log.Error().Err(err).Msg("Failed to handle keys")

			}

			// process error addresses
			for _, addr := range accountAddresses {
				log.Debug().Msgf("Process again address to error list: %v", addr)
				addressChan <- []flow.Address{flow.HexToAddress(addr)}
			}

			// add wait time in seconds
			time.Sleep(time.Duration(pauseInterval) * time.Second)
		}
	}()

	return nil
}
