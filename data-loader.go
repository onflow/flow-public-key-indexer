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
	"math/big"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog/log"
)

type DataLoader struct {
	db     Database
	config Params
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

//go:embed get_keys.cdc
var GetAccountKeys string

type GetAccountKeysHandler func(keys []PublicKey, err error)

func (s *DataLoader) NewGetAccountKeysHandler(handler GetAccountKeysHandler) func(value cadence.Value) {
	return func(value cadence.Value) {
		allAccountsKeys := []PublicKey{}
		for _, allKeys := range value.(cadence.Dictionary).Pairs {
			address := allKeys.Key.(cadence.Address)
			counter := 0
			keys := []PublicKey{}
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
				//handler(address, allKeys, nil)
				keys = append(keys, data)
				counter = counter + 1
			}
			allAccountsKeys = append(allAccountsKeys, keys...)
			if counter >= s.config.MaxAcctKeys {
				log.Info().Msgf("Account max keys reached %s %d %d", address.String(), counter, s.config.MaxAcctKeys)
			}
		}

		handler(allAccountsKeys, nil)
	}
}

func (s *DataLoader) ProcessAddressData(keys []PublicKey, err error) {
	if err != nil {
		log.Err(err).Msgf("failed to get account key info")
		return
	}
	pkis := convertPublicKey(keys)
	// send to db
	if err := s.db.BulkAddPublicKey(pkis); err != nil {
		log.Warn().Err(err).Msg("not able to save key information")
	}
}
func NewDataLoader(db Database, p Params) *DataLoader {
	s := DataLoader{}
	s.db = db
	s.config = p
	return &s
}

func (s *DataLoader) RunBulkLoader() (uint64, error) {
	config := DefaultConfig
	config.FlowAccessNodeURL = s.config.FlowUrl
	config.ChainID = flow.ChainID(s.config.ChainId)
	config.BatchSize = s.config.BatchSize
	config.ignoreZeroWeight = s.config.IgnoreZeroWeight
	config.ignoreRevoked = s.config.IgnoreRevoked
	config.ConcurrentClients = s.config.ConcurrenClients
	config.maxAcctKeys = s.config.MaxAcctKeys

	blockHeight, err := BatchScript(
		context.Background(),
		log.Logger,
		config,
		GetAccountKeys,
		s.NewGetAccountKeysHandler(s.ProcessAddressData),
	)
	return blockHeight, err
}

func (s *DataLoader) RunAddressLoader(addresses []string) (uint64, error) {
	config := DefaultConfig
	config.FlowAccessNodeURL = s.config.FlowUrl
	config.ChainID = flow.ChainID(s.config.ChainId)
	config.BatchSize = s.config.BatchSize
	config.ignoreZeroWeight = s.config.IgnoreZeroWeight
	config.ignoreRevoked = s.config.IgnoreRevoked
	config.ConcurrentClients = s.config.ConcurrenClients
	config.maxAcctKeys = s.config.MaxAcctKeys

	blockHeight, err := BatchScriptAddresses(
		context.Background(),
		log.Logger,
		config,
		GetAccountKeys,
		s.NewGetAccountKeysHandler(s.ProcessAddressData),
		addresses,
	)
	return blockHeight, err
}

func convertPublicKey(pks []PublicKey) []PublicKeyIndexer {
	allPki := map[string]PublicKeyIndexer{}
	for _, publicKey := range pks {
		newAcct := makePublicKeyIndexer(publicKey)
		pki, found := allPki[publicKey.account]
		if !found {
			newPki := PublicKeyIndexer{}
			newPki.Accounts = []Account{newAcct}
			newPki.PublicKey = publicKey.publicKey
			allPki[publicKey.publicKey] = newPki
		} else {
			unique := true
			for _, pk := range pki.Accounts {
				if pk.Account == newAcct.Account && pk.KeyId == newAcct.KeyId {
					unique = false
					break
				}
			}
			if unique {
				pki.Accounts = append(pki.Accounts, newAcct)
			}
		}

	}
	values := make([]PublicKeyIndexer, 0, len(allPki))
	for _, tx := range allPki {
		values = append(values, tx)
	}
	return values

}

func makePublicKeyIndexer(pk PublicKey) Account {
	return Account{
		Account:     pk.account,
		KeyId:       int(pk.keyIndex.Int64()),
		Weight:      int(pk.weight) / 100000000, // convert to display value
		SigningAlgo: int(pk.signatureAlgorithm),
		HashingAlgo: int(pk.hashAlgorithm),
		IsRevoked:   pk.isRevoked,
	}
}
