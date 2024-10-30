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

package addresses

import (
	"context"
	"strings"
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/access"
	"github.com/rs/zerolog"
)

type AddressProvider struct {
	log              zerolog.Logger
	lastAddress      flow.Address
	generator        *flow.AddressGenerator
	lastAddressIndex uint
	currentIndex     uint
	client           access.Client
	chain            flow.ChainID
	pause            time.Duration
}

const endOfAccountsError = "storage used is not initialized or not initialized correctly"
const failedToGetStoragedUsed = "get storage used failed: failed to get storage used"

const accountStorageUsageScript = `
 access(all) fun main(address: Address): UInt64 {
   return getAccount(address).storage.used
 }
 `

// NewAddressProvider creates and initializes a new AddressProvider
func NewAddressProvider(
	ctx context.Context,
	log zerolog.Logger,
	chain flow.ChainID,
	client access.Client,
	pause time.Duration,
	startIndex uint,
) (*AddressProvider, error) {
	ap := &AddressProvider{
		log:          log,
		generator:    flow.NewAddressGenerator(chain),
		currentIndex: 1,
		client:       client,
		chain:        chain,
		pause:        pause,
	}

	lastAddressIndex, err := ap.initLastAddress(ctx, startIndex)
	if err != nil {
		return nil, err
	}

	ap.lastAddress = ap.indexToAddress(lastAddressIndex)
	ap.lastAddressIndex = lastAddressIndex
	return ap, nil
}

// initLastAddress uses bisection to get the last existing address.
func (p *AddressProvider) initLastAddress(ctx context.Context, startIndex uint) (uint, error) {
	searchStep := 0
	addressExistsAtIndex := func(index uint) (bool, error) {
		time.Sleep(p.pause)

		searchStep += 1
		address := p.indexToAddress(index)

		p.log.Debug().Str("Bulk address", address.Hex()).Msgf("Searching last address %d", index)
		// This script will fail with endOfAccountsError
		// if the account (address at given index) doesn't exist yet
		_, err := p.client.ExecuteScriptAtLatestBlock(
			ctx,
			[]byte(accountStorageUsageScript),
			[]cadence.Value{cadence.NewAddress(address)},
		)

		if err == nil {
			return true, nil
		}
		if strings.Contains(err.Error(), endOfAccountsError) {
			return false, nil
		}
		if strings.Contains(err.Error(), failedToGetStoragedUsed) {
			return false, nil
		}
		return false, err
	}

	// We assume address #2 exists
	lastAddressIndex, err := p.getLastAddress(startIndex, startIndex*2, false, addressExistsAtIndex)
	if err != nil {
		return 0, err
	}

	p.log.Info().
		Str("Bulk lastAddress", p.indexToAddress(lastAddressIndex).Hex()).
		Uint("numAccounts", lastAddressIndex).
		Int("stepsNeeded", searchStep).
		Msg("")

	return lastAddressIndex, nil
}

// getLastAddress is a recursive function that finds the last address. Will use max 2 * log2(number_of_addresses) steps
// If the last address is at index 7 the algorithm goes like this:
// (x,y) <- lower and upper index
// 0. start at (1,2); address exists at 2
// 1. (2,4): address exists at 4
// 2. (4,8): address doesnt exist at 8
// 3. (4,8): check address (8 - 4) / 2 = 6  address exists so next pair is (6,8)
// 4. (6,8): check address 7 address exists so next pair is (7,8)
// 5. (7,8): check address (8 - 7) / 2 = 7 ... ok already checked so this is the last existing address
func (p *AddressProvider) getLastAddress(
	lowerIndex uint,
	upperIndex uint,
	upperExists bool,
	addressExistsAtIndex func(uint) (bool, error),
) (uint, error) {

	// Does the address exist at upper bound?
	if upperExists {
		// double the upper bound, the current upper bound is now the lower upper bound
		newUpperIndex := upperIndex * 2
		newUpperExists, err := addressExistsAtIndex(newUpperIndex)
		if err != nil {
			return 0, err
		}
		return p.getLastAddress(upperIndex, newUpperIndex, newUpperExists, addressExistsAtIndex)
	}

	midIndex := (upperIndex-lowerIndex)/2 + lowerIndex
	if midIndex == lowerIndex {
		// we found the last address
		return midIndex, nil
	}

	// Check if the address exists in the middle of the interval.
	// If yes, then take the (mid, upper) as the next pair,
	// else take (lower, mid) as the next pair
	midIndexExists, err := addressExistsAtIndex(midIndex)
	if err != nil {
		return 0, err
	}
	if midIndexExists {
		return p.getLastAddress(midIndex, upperIndex, upperExists, addressExistsAtIndex)
	} else {
		return p.getLastAddress(lowerIndex, midIndex, midIndexExists, addressExistsAtIndex)
	}
}

func (p *AddressProvider) indexToAddress(index uint) flow.Address {
	p.generator.SetIndex(index)
	return p.generator.Address()
}

func (p *AddressProvider) getNextAddress() (address flow.Address, isOutOfBounds bool) {
	address = p.indexToAddress(p.currentIndex)

	// Give some progress information every so often
	if p.currentIndex%(p.lastAddressIndex/10) == 0 {
		p.log.Debug().Msgf("Bulk Processed %v %% accounts", p.currentIndex/(p.lastAddressIndex/10)*10)
	}

	if p.currentIndex > p.lastAddressIndex {
		isOutOfBounds = true
	}
	p.currentIndex += 1

	return
}

// These addresses are known to be broken on Mainnet
var brokenAddresses = map[flow.Address]struct{}{
	flow.HexToAddress("bf48a20670f179b8"): {},
	flow.HexToAddress("5eba0297874a2bfd"): {},
	flow.HexToAddress("474ec037bcd8accf"): {},
	flow.HexToAddress("b0e80595d267f4eb"): {},
}

// GenerateAddresses generates individual addresses and sends them to the provided channel
func (p *AddressProvider) GenerateAddresses(ctx context.Context, addressChan chan<- flow.Address) {
	for {
		addr, oob := p.getNextAddress()
		if oob {
			break
		}

		if _, ok := brokenAddresses[addr]; ok {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case addressChan <- addr:
			// Address sent successfully
		}
	}
	close(addressChan)
}

// GenerateAddressBatches generates batches of addresses and sends them to the provided channel
func (p *AddressProvider) GenerateAddressBatches(ctx context.Context, addressChan chan<- []flow.Address, batchSize int) {
	batchChan := make(chan flow.Address, batchSize)
	go p.GenerateAddresses(ctx, batchChan)

	var batch []flow.Address
	for addr := range batchChan {
		batch = append(batch, addr)
		if len(batch) == batchSize {
			select {
			case <-ctx.Done():
				return
			case addressChan <- batch:
				batch = nil
			}
		}
	}

	if len(batch) > 0 {
		addressChan <- batch
	}
	close(addressChan)
}
