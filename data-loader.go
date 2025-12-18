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
	"strings"
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/access"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	// Add this line to import the utils package
)

type PublicKey struct {
	hashAlgorithm      uint8
	isRevoked          bool
	weight             uint64
	keyIndex           int
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
	flowClient access.Client,
	fetchSlowDown int,
) ([]model.PublicKeyAccountIndexer, error) {
	script := []byte(GetAccountKeys)
	accountsCadenceValues := convertAddresses(addresses)
	arguments := []cadence.Value{cadence.NewArray(accountsCadenceValues), cadence.NewInt(conf.MaxAcctKeys), cadence.NewBool(conf.IgnoreZeroWeight), cadence.NewBool(conf.IgnoreRevoked)}
	result, err := retryScriptUntilSuccess(ctx, log, script, arguments, flowClient, time.Duration(fetchSlowDown)*time.Millisecond)

	if err != nil {
		log.Error().Err(err).Msg("Script: Failed to get account keys")
		return nil, err
	}

	keys, err := getAccountKeysFromCadence(result)
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

	if len(accountAddresses) > 0 {
		addrs := uniqueToFlowAddress(accountAddresses)
		log.Debug().Msgf("Inc addressChan: Before adding to channel, %d addresses, at %v", len(accountAddresses), synchedBlockHeight)

		addressChan <- addrs

		log.Debug().Msgf("Inc addressChan: After found %d addresses, at %v", len(accountAddresses), synchedBlockHeight)
	}

	return synchedBlockHeight, err
}

func uniqueToFlowAddress(addresses []string) []flow.Address {
	keys := make(map[string]bool)
	list := []flow.Address{}
	for _, entry := range addresses {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			flowAddress := flow.HexToAddress(entry)
			list = append(list, flowAddress)
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
	script []byte,
	arguments []cadence.Value,
	flowClient access.Client,
	pause time.Duration,
) (cadence.Value, error) {
	var err error
	var result cadence.Value
	attempts := 0
	maxAttemps := 5

	for {
		result, err = flowClient.ExecuteScriptAtLatestBlock(
			ctx,
			script,
			arguments,
		)

		if err == nil {
			break
		}
		attempts = attempts + 1
		log.Error().Err(err).Msgf("Script: %d attempt, %v", attempts, arguments)

		if attempts > maxAttemps || strings.Contains(err.Error(), "connection termination") {
			// give up and don't retry
			break
		}

		time.Sleep(pause)

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

func getAccountKeysFromCadence(value cadence.Value) ([]model.PublicKeyAccountIndexer, error) {
	allAccountsKeys := []model.PublicKeyAccountIndexer{}

	// Safe type assertion for the top-level dictionary
	dict, ok := value.(cadence.Dictionary)
	if !ok {
		log.Warn().Msgf("Script result is not a Dictionary, got type: %T", value)
		return allAccountsKeys, nil
	}

	for _, allKeys := range dict.Pairs {
		address, addrOk := allKeys.Key.(cadence.Address)
		if !addrOk {
			log.Warn().Msgf("Key is not an Address, got type: %T", allKeys.Key)
			continue
		}
		accountAddress := address.String()
		keys := []model.PublicKeyAccountIndexer{}

		keysDict, keysOk := allKeys.Value.(cadence.Dictionary)
		if !keysOk {
			log.Warn().Msgf("Keys value for address %s is not a Dictionary, got type: %T", accountAddress, allKeys.Value)
			continue
		}

		for _, nameCodePair := range keysDict.Pairs {
			rawStruct, structOk := nameCodePair.Value.(cadence.Struct)
			if !structOk {
				log.Warn().Msgf("Key value is not a Struct for address %s, got type: %T", accountAddress, nameCodePair.Value)
				continue
			}

			fields := rawStruct.FieldsMappedByName()

			// Safe field extraction with type checking
			hashAlgoVal, ok1 := fields["hashAlgorithm"].(cadence.UInt8)
			isRevokedVal, ok2 := fields["isRevoked"].(cadence.Bool)
			weightVal, ok3 := fields["weight"].(cadence.UFix64)
			publicKeyVal, ok4 := fields["publicKey"].(cadence.String)
			keyIndexVal, ok5 := fields["keyIndex"].(cadence.Int)
			sigAlgoVal, ok6 := fields["signatureAlgorithm"].(cadence.UInt8)

			if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 || !ok6 {
				log.Error().Msgf("Field type mismatch for address %s. Types: hashAlgorithm=%T, isRevoked=%T, weight=%T, publicKey=%T, keyIndex=%T, signatureAlgorithm=%T",
					accountAddress, fields["hashAlgorithm"], fields["isRevoked"], fields["weight"], fields["publicKey"], fields["keyIndex"], fields["signatureAlgorithm"])
				continue
			}

			data := PublicKey{
				hashAlgorithm:      uint8(hashAlgoVal),
				isRevoked:          bool(isRevokedVal),
				weight:             uint64(weightVal),
				publicKey:          string(publicKeyVal),
				keyIndex:           int(keyIndexVal.Int()),
				signatureAlgorithm: uint8(sigAlgoVal),
				account:            accountAddress,
			}

			item := model.PublicKeyAccountIndexer{
				Account:   data.account,
				KeyId:     int(data.keyIndex),
				PublicKey: data.publicKey,
				Weight:    int(data.weight / 100000000),
				SigAlgo:   int(data.signatureAlgorithm),
				HashAlgo:  int(data.hashAlgorithm),
				IsRevoked: data.isRevoked,
			}

			keys = append(keys, item)
		}
		allAccountsKeys = append(allAccountsKeys, keys...)
	}

	return allAccountsKeys, nil
}
