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
	"fmt"
	"sync"
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	flowclient "github.com/onflow/flow-go-sdk/client"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
	AtBlockHeight     uint64
	FlowAccessNodeURL string
	ConcurrentClients int
	Pause             time.Duration
	ChainID           flow.ChainID
	maxAcctKeys       int
	ignoreZeroWeight  bool
	ignoreRevoked     bool
}

var DefaultConfig = Config{
	BatchSize:         100,
	AtBlockHeight:     0,
	FlowAccessNodeURL: "access.mainnet.nodes.onflow.org:9000",
	ConcurrentClients: 2, // should be a good number to not produce too much traffic
	Pause:             0,
	ChainID:           "chain-mainnet",
}

func BatchScript(
	ctx context.Context,
	log zerolog.Logger,
	conf Config,
	script string,
	handler func(cadence.Value),
) (height uint64, err error) {
	code := []byte(script)

	flowClient := getFlowClient(conf.FlowAccessNodeURL)

	currentBlock, err := getBlockHeight(ctx, conf, flowClient)
	if err != nil {
		return 0, err
	}

	ap, err := InitAddressProvider(ctx, log, conf.ChainID, currentBlock.ID, flowClient, conf.Pause)
	if err != nil {
		return 0, err
	}

	wg := &sync.WaitGroup{}
	addressChan := make(chan []flow.Address)

	for i := 0; i < conf.ConcurrentClients; i++ {
		wg.Add(1)
		go func() {
			// Each worker has a separate Flow client
			client := getFlowClient(conf.FlowAccessNodeURL)
			defer func() {
				err = client.Close()
				if err != nil {
					log.Warn().
						Err(err).
						Msg("error closing client")
				}
			}()

			// Get the batches of address through addressChan,
			// run the script with that batch of addresses,
			// and pass the result to the handler

			for accountAddresses := range addressChan {
				accountsCadenceValues := convertAddresses(accountAddresses)
				arguments := []cadence.Value{cadence.NewArray(accountsCadenceValues), cadence.NewInt(conf.maxAcctKeys), cadence.NewBool(conf.ignoreZeroWeight), cadence.NewBool(conf.ignoreRevoked)}
				result := retryScriptUntilSuccess(ctx, log, currentBlock.Height, code, arguments, client)
				handler(result)
			}

			wg.Done()
		}()
	}

	ap.GenerateAddressBatches(addressChan, conf.BatchSize)

	// Close the addressChan and wait for the workers to finish
	close(addressChan)
	wg.Wait()

	return currentBlock.Height, nil
}

func BatchScriptAddresses(
	ctx context.Context,
	log zerolog.Logger,
	conf Config,
	script string,
	handler func(cadence.Value),
	addresses []string,
) (height uint64, err error) {
	code := []byte(script)

	var fetchAddresses []flow.Address

	for _, addr := range addresses {
		a := flow.HexToAddress(addr)
		fetchAddresses = append(fetchAddresses, a)
	}

	batchedAddrs := BatchAddresses(fetchAddresses, conf.BatchSize)
	flowClient := getFlowClient(conf.FlowAccessNodeURL)

	currentBlock, err := getBlockHeight(ctx, conf, flowClient)
	if err != nil {
		log.Warn().Err(err).Msg("Issue getting block height")
		return 0, err
	}

	wg := &sync.WaitGroup{}

	for i := 0; i < conf.ConcurrentClients; i++ {
		wg.Add(1)
		go func() {
			// Each worker has a separate Flow client
			client := getFlowClient(conf.FlowAccessNodeURL)
			defer func() {
				err = client.Close()
				if err != nil {
					log.Warn().
						Err(err).
						Msg("error closing client")
				}
			}()

			// Get the batches of address through addressChan,
			// run the script with that batch of addresses,
			// and pass the result to the handler

			for _, accountAddresses := range batchedAddrs {
				accountsCadenceValues := convertAddresses(accountAddresses)
				arguments := []cadence.Value{cadence.NewArray(accountsCadenceValues), cadence.NewInt(conf.maxAcctKeys), cadence.NewBool(conf.ignoreZeroWeight), cadence.NewBool(conf.ignoreRevoked)}
				result := retryScriptUntilSuccess(ctx, log, currentBlock.Height, code, arguments, client)
				handler(result)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return currentBlock.Height, nil
}

func GetAllAddresses(
	ctx context.Context,
	log zerolog.Logger,
	conf Config,
	addressChan chan []flow.Address,
) (height uint64, err error) {
	flowClient := getFlowClient(conf.FlowAccessNodeURL)

	currentBlock, err := getBlockHeight(ctx, conf, flowClient)
	if err != nil {
		return 0, err
	}

	ap, err := InitAddressProvider(ctx, log, conf.ChainID, currentBlock.ID, flowClient, conf.Pause)
	if err != nil {
		return 0, err
	}
	ap.GenerateAddressBatches(addressChan, conf.BatchSize)

	return currentBlock.Height, nil
}

func RunAddressCadenceScript(
	ctx context.Context,
	log zerolog.Logger,
	conf Config,
	script string,
	handler func(cadence.Value),
	addressChan chan []flow.Address,
) (height uint64, err error) {
	code := []byte(script)

	flowClient := getFlowClient(conf.FlowAccessNodeURL)

	currentBlock, err := getBlockHeight(ctx, conf, flowClient)
	if err != nil {
		return 0, err
	}

	for i := 0; i < conf.ConcurrentClients; i++ {
		go func() {
			// Each worker has a separate Flow client
			client := getFlowClient(conf.FlowAccessNodeURL)
			defer func() {
				err = client.Close()
				if err != nil {
					log.Warn().
						Err(err).
						Msg("error closing client")
				}
			}()

			// Get the batches of address through addressChan,
			// run the script with that batch of addresses,
			// and pass the result to the handler

			for accountAddresses := range addressChan {
				accountsCadenceValues := convertAddresses(accountAddresses)
				arguments := []cadence.Value{cadence.NewArray(accountsCadenceValues), cadence.NewInt(conf.maxAcctKeys), cadence.NewBool(conf.ignoreZeroWeight), cadence.NewBool(conf.ignoreRevoked)}
				result := retryScriptUntilSuccess(ctx, log, currentBlock.Height, code, arguments, client)
				handler(result)
			}
		}()
	}

	return currentBlock.Height, nil
}

func getBlockHeight(ctx context.Context, conf Config, flowClient *flowclient.Client) (*flow.BlockHeader, error) {
	if conf.AtBlockHeight != 0 {
		blk, err := flowClient.GetBlockByHeight(ctx, conf.AtBlockHeight)
		if err != nil {
			return nil, fmt.Errorf("failed to get block at the specified height: %w", err)
		}
		return &blk.BlockHeader, nil
	} else {
		block, err := flowClient.GetLatestBlockHeader(ctx, true)
		if err != nil {
			return nil, fmt.Errorf("failed to get the latest block header: %w", err)
		}
		log.Debug().Uint64("blockHeight", block.Height).Msg("Fetched block info")
		return block, nil
	}
}

// retryScriptUntilSuccess retries running the cadence script until we get a successful response back,
// returning an array of balance pairs, along with a boolean representing whether we can continue
// or are finished processing.
func retryScriptUntilSuccess(
	ctx context.Context,
	log zerolog.Logger,
	blockHeight uint64,
	script []byte,
	arguments []cadence.Value,
	flowClient *flowclient.Client,
) (result cadence.Value) {
	var err error

	for {
		time.Sleep(10 * time.Millisecond)

		result, err = flowClient.ExecuteScriptAtBlockHeight(
			ctx,
			blockHeight,
			script,
			arguments,
			grpc.MaxCallRecvMsgSize(16*1024*1024),
		)
		if err == nil {
			break
		}

		log.Warn().Msgf("received unknown error, retrying: %s", err.Error())
	}

	return result
}

func convertAddresses(addresses []flow.Address) []cadence.Value {
	var accounts []cadence.Value
	for _, address := range addresses {
		accounts = append(accounts, cadence.Address(address))
	}
	return accounts
}
