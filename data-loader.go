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
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog/log"
)

type DataLoader struct {
	DB             pg.Store
	config         Params
	fa             FlowAdapter
	incAddressChan [][]flow.Address
}

type PublicKeyActions struct {
	removes   []PublicKeyEvent
	addresses []string
}

type PublicKeyEvent struct {
	publicKey string
	account   string
}

type PublicKey struct {
	hashAlgorithm      uint8
	isRevoked          bool
	weight             uint64
	keyIndex           *big.Int
	publicKey          string
	signatureAlgorithm uint8
	account            string
}

//go:embed cadence/get_keys.cdc
var GetAccountKeys string

type GetAccountKeysHandler func(keys []model.PublicKeyAccountIndexer, height uint64, err error)

func (s *DataLoader) NewGetAccountKeysHandler(handler GetAccountKeysHandler) func(value cadence.Value, height uint64) {
	return func(value cadence.Value, height uint64) {
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
			if counter >= s.config.MaxAcctKeys {
				log.Debug().Msgf("Account max keys reached %s %d", address.String(), s.config.MaxAcctKeys)
			}
		}

		handler(allAccountsKeys, height, nil)
	}
}

func NewDataLoader(DB pg.Store, fa FlowAdapter, p Params) *DataLoader {
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

func (s *DataLoader) RunAllAddressesLoader(addressChan chan []flow.Address, block *flow.BlockHeader) error {
	config := DefaultConfig
	config.FlowAccessNodeURLs = s.config.AllFlowUrls
	config.ChainID = flow.ChainID(s.config.ChainId)
	config.BatchSize = s.config.BatchSize
	config.ignoreZeroWeight = s.config.IgnoreZeroWeight
	config.ignoreRevoked = s.config.IgnoreRevoked
	config.maxAcctKeys = s.config.MaxAcctKeys
	config.Pause = time.Duration(s.config.FetchSlowDownMs * int(time.Millisecond))

	_, err := GetAllAddresses(context.Background(), log.Logger, config, addressChan, block)
	return err
}

func (s *DataLoader) RunIncAddressesLoader(addressChan chan []flow.Address, blockHeight uint64) (int, bool) {
	pkAddrActions, currBlockHeight, restart, err := s.fa.GetAddressesFromBlockEvents(s.config.AllFlowUrls, blockHeight, s.config.MaxBlockRange, s.config.WaitNumBlocks)
	if err != nil {
		return 0, restart
	}

	s.DB.UpdatePendingBlockHeight(currBlockHeight)
	var addresses []flow.Address
	// filter out empty Public keys and send account addresses to get processed
	// debug log num addresses found
	log.Debug().Msgf("found %d addresses", len(pkAddrActions))
	for _, addrPkAction := range pkAddrActions {
		for _, address := range addrPkAction.addresses {
			// reload these addresses to get all account data
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
		log.Debug().Msgf("bb found %d addresses", len(addresses))
		addressChan <- unique(addresses)
	}

	updatedToBlock, _ := s.DB.GetUpdatedBlockHeight()
	if updatedToBlock != 0 {
		// bulk loading is done, update loaded block height
		s.DB.UpdateUpdatedBlockHeight(currBlockHeight)
	}
	// cache distict count for quick 'status' response
	s.DB.UpdateDistinctCount()
	return len(addresses), restart
}

func (s *DataLoader) ProcessAddressDataAdditions(keys []model.PublicKeyAccountIndexer, height uint64, err error) {
	if err != nil {
		log.Err(err).Msgf("failed to get account key info")
		return
	}

	// send to DB
	s.DB.InsertPublicKeyAccounts(keys)
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
