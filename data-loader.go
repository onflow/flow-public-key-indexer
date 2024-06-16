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
	"math/big"
	"strings"
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	flowclient "github.com/onflow/flow-go-sdk/client"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

type PublicKey struct {
	hashAlgorithm      uint8
	isRevoked          bool
	weight             uint64
	keyIndex           *big.Int
	publicKey          string
	signatureAlgorithm uint8
	account            string
}

type DataLoader struct {
	DB             pg.Store
	config         Params
	fa             FlowAdapter
	incAddressChan [][]flow.Address
}

func NewDataLoader(DB pg.Store, fa FlowAdapter, p Params) *DataLoader {
	s := DataLoader{}
	s.incAddressChan = [][]flow.Address{}
	s.DB = DB
	s.fa = fa
	s.config = p
	return &s
}

//go:embed cadence/get_keys.cdc
var GetAccountKeys string

func ProcessAddressWithScript(
	ctx context.Context,
	conf Params,
	addresses []flow.Address,
	log zerolog.Logger,
	flowClient *flowclient.Client,
	fetchSlowDown int,
	currentBlockHeight uint64,
) ([]model.PublicKeyAccountIndexer, error) {
	script := []byte(GetAccountKeys)
	accountsCadenceValues := convertAddresses(addresses)
	arguments := []cadence.Value{cadence.NewArray(accountsCadenceValues), cadence.NewInt(conf.MaxAcctKeys), cadence.NewBool(conf.IgnoreZeroWeight), cadence.NewBool(conf.IgnoreRevoked)}
	result, err := retryScriptUntilSuccess(ctx, log, currentBlockHeight, script, arguments, flowClient, time.Duration(fetchSlowDown)*time.Millisecond)

	if err != nil {
		log.Error().Err(err).Msg("Script: Failed to get account keys")
		return nil, err
	}

	keys, err := getAccountKeys(result)
	if err != nil {
		log.Error().Err(err).Msg("Script: Failed to get account keys")
	}
	return keys, err
}

func convertAddresses(addresses []flow.Address) []cadence.Value {
	var accounts []cadence.Value
	for _, address := range addresses {
		accounts = append(accounts, cadence.Address(address))
	}
	return accounts
}

func (s *DataLoader) RunIncAddressesLoader(addressChan chan []flow.Address, blockHeight uint64, endBlockHeight uint64) (uint64, error) {
	accountAddresses, synchedBlockHeight, err := s.fa.GetAddressesFromBlockEvents(s.config.AllFlowUrls, blockHeight, endBlockHeight)
	if err != nil {
		return blockHeight, err
	}

	var addresses []flow.Address
	// filter out empty Public keys and send account addresses to get processed
	// debug log num addresses found
	for _, accountAddr := range accountAddresses {
		s.DB.RemoveAccountForReloading(add0xPrefix(accountAddr))
		addresses = append(addresses, flow.HexToAddress(accountAddr))
	}

	if len(addresses) > 0 {
		addrs := unique(addresses)
		log.Debug().Msgf("Inc: addressChan: Before adding to channel, %d addresses, at %v", len(addresses), synchedBlockHeight)

		addressChan <- addrs

		log.Debug().Msgf("Inc: addressChan: After found %d addresses, at %v", len(addresses), synchedBlockHeight)
	}

	return synchedBlockHeight, err
}

func unique(addresses []flow.Address) []flow.Address {
	keys := make(map[flow.Address]bool)
	list := []flow.Address{}
	for _, entry := range addresses {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

// retries running the cadence script until we get a successful response back,
// returning an array, along with a boolean representing whether we can continue
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

	for {
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
		log.Error().Err(err).Msgf("Script: %d attempt", attempts)

		if attempts > maxAttemps || strings.Contains(err.Error(), "connection termination") {
			// give up and don't retry
			break
		}

		time.Sleep(pause)
		block, _ := flowClient.GetLatestBlockHeader(ctx, true)
		blockHeight = block.Height

		if strings.Contains(err.Error(), "ResourceExhausted") {
			// really slow down when node is ResourceExhausted
			continue
		}
		if strings.Contains(err.Error(), "InvalidArgument") {
			// really slow down when node is ResourceExhausted
			continue
		}
		if strings.Contains(err.Error(), "DeadlineExceeded") {
			// pass error back to caller, script ran too long
			break
		}
	}

	return result, err
}

func getAccountKeys(value cadence.Value) ([]model.PublicKeyAccountIndexer, error) {
	allAccountsKeys := []model.PublicKeyAccountIndexer{}
	for _, allKeys := range value.(cadence.Dictionary).Pairs {
		address := allKeys.Key.(cadence.Address)
		counter := 0
		keys := []model.PublicKeyAccountIndexer{}
		for _, nameCodePair := range allKeys.Value.(cadence.Dictionary).Pairs {
			rawStruct := nameCodePair.Value.(cadence.Struct)
			data := PublicKey{
				hashAlgorithm:      rawStruct.Fields[0].ToGoValue().(uint8),
				isRevoked:          rawStruct.Fields[1].ToGoValue().(bool),
				weight:             rawStruct.Fields[2].ToGoValue().(uint64),
				publicKey:          rawStruct.Fields[3].ToGoValue().(string),
				keyIndex:           rawStruct.Fields[4].ToGoValue().(*big.Int),
				signatureAlgorithm: rawStruct.Fields[5].ToGoValue().(uint8),
				account:            address.String(),
			}
			item := model.PublicKeyAccountIndexer{
				Account:   data.account,
				KeyId:     int(data.keyIndex.Int64()),
				PublicKey: data.publicKey,
				Weight:    int(data.weight / 100000000),
			}
			keys = append(keys, item)
			counter = counter + 1
		}
		allAccountsKeys = append(allAccountsKeys, keys...)
	}

	return allAccountsKeys, nil
}
