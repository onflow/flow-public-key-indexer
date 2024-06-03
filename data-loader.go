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
	"example/flow-key-indexer/pkg/pg"
	"time"

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
	addresses []string
}

type PublicKeyEvent struct {
	publicKey string
	account   string
}

func NewDataLoader(DB pg.Store, fa FlowAdapter, p Params) *DataLoader {
	s := DataLoader{}
	s.incAddressChan = [][]flow.Address{}
	s.DB = DB
	s.fa = fa
	s.config = p
	return &s
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

	_, err := GetAllAddresses(context.Background(), log.Logger, config, addressChan, block, s.DB.AddressIsValid)
	return err
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
		log.Debug().Msgf("addressChan: before adding to channel, %d addresses, at %v", len(addresses), synchedBlockHeight)
		// processing account key address
		addressChan <- addrs
		log.Debug().Msgf("addressChan: after found %d addresses, at %v", len(addresses), synchedBlockHeight)
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
