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
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog/log"
)

type DataLoader struct {
	DB             Database
	config         Params
	fa             FlowAdapter
	incAddressChan [][]flow.Address
}

type PublicKey struct {
	publicKey string
	account   string
}

//go:embed get_pub_keys.cdc
var GetAccountKeys string

type GetAccountKeysHandler func(keys []PublicKey, height uint64, err error)

func (s *DataLoader) NewGetAccountKeysHandler(handler GetAccountKeysHandler) func(value cadence.Value, height uint64) {
	return func(value cadence.Value, height uint64) {
		allAccountsKeys := []PublicKey{}
		for _, allKeys := range value.(cadence.Dictionary).Pairs {
			address := allKeys.Key.(cadence.Address)
			counter := 0
			keys := []PublicKey{}
			for _, nameCodePair := range allKeys.Value.(cadence.Array).Values {
				rawStruct := nameCodePair.(cadence.String)
				data := PublicKey{
					publicKey: rawStruct.ToGoValue().(string),
					account:   address.String(),
				}
				keys = append(keys, data)
				counter = counter + 1
			}
			allAccountsKeys = append(allAccountsKeys, keys...)
			if counter >= s.config.MaxAcctKeys {
				log.Debug().Msgf("Account max keys reached %s %d", address.String(), s.config.MaxAcctKeys)
			}
		}

		handler(allAccountsKeys, height, nil)
	}
}

func NewDataLoader(DB Database, fa FlowAdapter, p Params) *DataLoader {
	s := DataLoader{}
	s.incAddressChan = [][]flow.Address{}
	s.DB = DB
	s.fa = fa
	s.config = p
	return &s
}

func (s *DataLoader) SetupAddressLoader(addressChan chan []flow.Address) (uint64, error) {
	config := DefaultConfig
	config.FlowAccessNodeURLs = s.config.AllFlowUrls
	config.ChainID = flow.ChainID(s.config.ChainId)
	config.BatchSize = s.config.BatchSize
	config.ignoreZeroWeight = s.config.IgnoreZeroWeight
	config.ignoreRevoked = s.config.IgnoreRevoked
	config.maxAcctKeys = s.config.MaxAcctKeys

	blockHeight, err := RunAddressCadenceScript(
		context.Background(),
		log.Logger,
		config,
		GetAccountKeys,
		s.NewGetAccountKeysHandler(s.ProcessAddressData),
		addressChan,
	)
	return blockHeight, err
}

func (s *DataLoader) RunAllAddressesLoader(addressChan chan []flow.Address) error {
	config := DefaultConfig
	config.FlowAccessNodeURLs = s.config.AllFlowUrls
	config.ChainID = flow.ChainID(s.config.ChainId)
	config.BatchSize = s.config.BatchSize
	config.ignoreZeroWeight = s.config.IgnoreZeroWeight
	config.ignoreRevoked = s.config.IgnoreRevoked
	config.maxAcctKeys = s.config.MaxAcctKeys
	config.Pause = time.Duration(s.config.FetchSlowDownMs * int(time.Millisecond))

	s.DB.updateUpdatedBlockHeight(0) // indicates bulk loading
	height, err := GetAllAddresses(context.Background(), log.Logger, config, addressChan)
	// save height for next load time
	s.DB.updateUpdatedBlockHeight(height)
	return err
}

func (s *DataLoader) RunIncAddressesLoader(addressChan chan []flow.Address, isLoading bool, blockHeight uint64) (int, bool) {
	addresses, currBlockHeight, restart := s.fa.GetAddressesFromBlockEvents(s.config.AllFlowUrls, blockHeight, s.config.MaxBlockRange, s.config.WaitNumBlocks)
	s.DB.updateLoadingBlockHeight(currBlockHeight)
	addressChan <- addresses
	s.DB.CleanUp()
	updatedToBlock, _ := s.DB.GetUpdatedBlockHeight()
	if updatedToBlock != 0 {
		// bulk loading is done, update loaded block height
		s.DB.updateUpdatedBlockHeight(currBlockHeight)
	}
	return len(addresses), restart
}

func (s *DataLoader) ProcessAddressData(keys []PublicKey, height uint64, err error) {
	if err != nil {
		log.Err(err).Msgf("failed to get account key info")
		return
	}

	pkis := convertPublicKey(keys, height)
	// send to DB
	s.DB.UpdatePublicKeys(pkis)
}

func convertPublicKey(pks []PublicKey, height uint64) []PublicKeyIndexer {
	allPki := map[string]PublicKeyIndexer{}
	for _, publicKey := range pks {
		newAcct := publicKey.account
		if pki, found := allPki[publicKey.publicKey]; found {
			if !contains(pki.Accounts, newAcct) {
				pki.Accounts = append(pki.Accounts, newAcct)
				allPki[pki.PublicKey] = pki
			}
		} else {
			newPki := PublicKeyIndexer{}
			newPki.Accounts = []string{newAcct}
			newPki.PublicKey = publicKey.publicKey
			allPki[publicKey.publicKey] = newPki
		}
	}
	values := make([]PublicKeyIndexer, 0, len(allPki))
	for _, pi := range allPki {
		values = append(values, pi)
	}
	return values
}
