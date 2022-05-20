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
	"strings"
	"time"

	"github.com/onflow/cadence"
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
) (height uint64, err error) {
	flowClient := getFlowClient(conf.FlowAccessNodeURLs[0])

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
	handler func(cadence.Value, uint64),
	addressChan chan []flow.Address,
) (height uint64, err error) {
	code := []byte(script)
	flowUrl := conf.FlowAccessNodeURLs[0]
	flowClient := getFlowClient(flowUrl)

	currentBlock, err := getBlockHeight(ctx, conf, flowClient)
	if err != nil {
		return 0, err
	}

	for i := 0; i < len(conf.FlowAccessNodeURLs); i++ {
		flowUrl := conf.FlowAccessNodeURLs[i]
		log.Debug().Msgf("using flow url %v", flowUrl)
		go func() {
			// Each worker has a separate Flow client
			client := getFlowClient(flowUrl)
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
				runScript(ctx, conf, accountAddresses, log, code, flowClient, handler)
			}
		}()
	}

	return currentBlock.Height, nil
}

func runScript(
	ctx context.Context,
	conf Config,
	addresses []flow.Address,
	log zerolog.Logger,
	script []byte,
	flowClient *flowclient.Client,
	handler func(cadence.Value, uint64),
) {
	currentBlock, _ := getBlockHeight(ctx, conf, flowClient)
	accountsCadenceValues := convertAddresses(addresses)
	arguments := []cadence.Value{cadence.NewArray(accountsCadenceValues), cadence.NewInt(conf.maxAcctKeys), cadence.NewBool(conf.ignoreZeroWeight), cadence.NewBool(conf.ignoreRevoked)}
	result, err := retryScriptUntilSuccess(ctx, log, currentBlock.Height, script, arguments, flowClient, conf.Pause)

	if err != nil {
		log.Error().Err(err).Msgf("long running script error, reducing num accounts. (%d addr)", len(accountsCadenceValues))
		for _, newAddresses := range splitAddr(addresses) {
			log.Warn().Msgf("rerunning script with fewer addresses (%d addr)", len(newAddresses))
			runScript(ctx, conf, newAddresses, log, script, flowClient, handler)
		}
	} else {
		handler(result, currentBlock.Height)
	}
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
	pause time.Duration,
) (cadence.Value, error) {
	var err error
	var result cadence.Value
	attempts := 0
	maxAttemps := 5
	last := time.Now()

	for {
		if time.Since(last) < pause {
			time.Sleep(pause)
		}
		last = time.Now()

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
		attempts = attempts + 1
		// really slow down when node is ResourceExhausted
		if strings.Contains(err.Error(), "ResourceExhausted") {
			log.Info().Msgf("server exhausted, taking a second rest. (%d req)", attempts)
			time.Sleep(2 * pause)
			continue
		}
		if strings.Contains(err.Error(), "DeadlineExceeded") {
			// pass error back to caller
			break
		}

		log.Error().Err(err).Msgf("unknown error, retrying: (%d attempt)", attempts)
		if attempts == maxAttemps {
			break
		}
	}

	return result, err
}

func convertAddresses(addresses []flow.Address) []cadence.Value {
	var accounts []cadence.Value
	for _, address := range addresses {
		accounts = append(accounts, cadence.Address(address))
	}
	return accounts
}

func splitAddr(addresses []flow.Address) [][]flow.Address {
	limit := int(len(addresses) / 2)
	var temp2 []flow.Address
	var temp []flow.Address

	for i := 0; i < len(addresses); i++ {
		addr := addresses[i]
		if i < limit {
			temp = append(temp, addr)
		} else {
			temp2 = append(temp2, addr)
		}
	}
	return [][]flow.Address{temp, temp2}
}
