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

type PublicKeyActions struct {
	adds      []PublicKey
	removes   []PublicKey
	addresses []string
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
		if value == nil {
			log.Warn().Msgf("cadence value is nil, nothing to process")
			return
		}
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
		s.NewGetAccountKeysHandler(s.ProcessAddressDataAdditions),
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
	pkAddrActions, currBlockHeight, restart, err := s.fa.GetAddressesFromBlockEvents(s.config.AllFlowUrls, blockHeight, s.config.MaxBlockRange, s.config.WaitNumBlocks)
	if err != nil {
		return 0, restart
	}
	s.DB.updateLoadingBlockHeight(currBlockHeight)
	var pkAddrs []PublicKey
	var addresses []flow.Address
	// filter out empty Public keys and send account addresses to get processed
	for _, addrPkAction := range pkAddrActions {
		pkAddrs = append(pkAddrs, addrPkAction.adds...)
		for _, address := range addrPkAction.addresses {
			// reload these addresses for removals and cuz of new AccountKeyAdded event format
			addresses = append(addresses, flow.HexToAddress(address))
		}
		for _, removal := range addrPkAction.removes {
			if removal.publicKey != "" {
				log.Debug().Msgf("removing %v %v", removal.publicKey, removal.account)
				s.DB.RemovePublicKeyInfo(removal.publicKey, removal.account)
			}
		}
	}

	if len(addresses) > 0 {
		// processing account key address
		addressChan <- unique(addresses)
	}

	if (len(pkAddrs)) > 0 {
		s.ProcessAddressDataAdditions(pkAddrs, currBlockHeight, nil)
	}

	if updatedBlock, updatedErr := s.DB.GetUpdatedBlockHeight(); updatedErr == nil {
		// check that bulk loading isn't occuring
		if updatedBlock != 0 {
			s.DB.CleanUp()
		}
	}

	updatedToBlock, _ := s.DB.GetUpdatedBlockHeight()
	if updatedToBlock != 0 {
		// bulk loading is done, update loaded block height
		s.DB.updateUpdatedBlockHeight(currBlockHeight)
	}
	return len(pkAddrs), restart
}

func (s *DataLoader) ProcessAddressDataAdditions(keys []PublicKey, height uint64, err error) {
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
