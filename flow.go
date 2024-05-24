package main

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/onflow/cadence"
	jsoncdc "github.com/onflow/cadence/encoding/json"
	"github.com/onflow/flow-go-sdk"
	flowGrpc "github.com/onflow/flow-go-sdk/access/grpc"
)

type FlowAdapter struct {
	Client  *flowGrpc.Client
	Context context.Context
	URL     string
}

type EventRangeQuery struct {
	Type        string
	StartHeight uint64
	EndHeight   uint64
}

func NewFlowClient(url string) *FlowAdapter {
	adapter := FlowAdapter{}
	adapter.Context = context.Background()
	// any reason to pass this as an arg instead?
	adapter.URL = url

	// create flow client
	FlowClient, err := flowGrpc.NewClient(url)
	if err != nil {
		log.Panic().Msgf("failed to connect to %s", adapter.URL)
	}
	adapter.Client = FlowClient
	return &adapter
}

func (fa *FlowAdapter) GetAccountAtBlockHeight(addr string, blockheight uint64) (*flow.Account, error) {
	hexAddr := flow.HexToAddress(addr)
	return fa.Client.GetAccountAtBlockHeight(fa.Context, hexAddr, blockheight)
}

func (fa *FlowAdapter) GetCurrentBlockHeight() (uint64, error) {
	block, err := fa.Client.GetLatestBlock(fa.Context, true)
	if err != nil {
		return 0, err
	}
	return block.Height, nil
}

func (fa *FlowAdapter) GetAddressesFromBlockEvents(flowClient *flowGrpc.Client, startBlockheight uint64, maxBlockRange int, waitNumBlocks int) ([]PublicKeyActions, uint64, bool, error) {
	itemsPerRequest := 245 // access node can only handel 250
	eventTypes := []string{"flow.AccountKeyAdded", "flow.AccountKeyRemoved"}
	var addrActions []PublicKeyActions
	restartBulkLoad := true
	currentHeight, err := fa.GetCurrentBlockHeight()
	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
		return addrActions, currentHeight, restartBulkLoad, err
	}

	// backing off a few blocks to allow for buffer
	BlockHeight := currentHeight //- 10 // backoff current
	// create chunks to make sure query limit is not exceeded
	var chunksEvents []EventRangeQuery

	for _, eventType := range eventTypes {
		events := ChunkEventRangeQuery(itemsPerRequest, startBlockheight, BlockHeight, eventType)
		chunksEvents = append(chunksEvents, events...)
	}

	addrs, restartDataLoader := fa.GetEventAddresses(flowClient, chunksEvents)
	return addrs, BlockHeight, restartDataLoader, nil
}

func ChunkEventRangeQuery(itemsPerRequest int, lowBlockHeight, highBlockHeight uint64, eventName string) []EventRangeQuery {
	maxServerBlockRange := float64(itemsPerRequest) // less than server's max of 250
	var chunks []EventRangeQuery
	totalRange := float64(highBlockHeight - lowBlockHeight)
	numChunks := math.Ceil(totalRange / maxServerBlockRange)
	lowHeight := lowBlockHeight
	highHeight := uint64(lowHeight + uint64(maxServerBlockRange))
	for i := 0; i < int(numChunks); i++ {
		query := EventRangeQuery{
			Type:        eventName,
			StartHeight: lowHeight,
			EndHeight:   highHeight,
		}
		chunks = append(chunks, query)
		lowHeight = lowHeight + uint64(maxServerBlockRange) + 1
		highHeight = lowHeight + uint64(maxServerBlockRange)

		if highHeight > highBlockHeight {
			highHeight = highBlockHeight
		}
	}
	return chunks
}

func RunAddressQuery(client *flowGrpc.Client, context context.Context, query EventRangeQuery) (PublicKeyActions, bool) {
	var publicKeyActions PublicKeyActions
	restartBulkLoad := false
	events, err := client.GetEventsForHeightRange(context, query.Type, query.StartHeight, query.EndHeight)
	if err != nil {
		log.Warn().Err(err).Msgf("retrying get events in block range %d %d", query.StartHeight, query.EndHeight)
		// break up query into smaller chunks
		for _, q := range splitQuery(query) {
			eventsRetry, errRetry := client.GetEventsForHeightRange(context, q.Type, q.StartHeight, q.EndHeight)
			if errRetry != nil {
				log.Error().Err(errRetry).Msg("retrying get events failed")
				return publicKeyActions, true
			}
			events = append(events, eventsRetry...)
		}
	}

	for _, event := range events {
		for _, evt := range event.Events {
			var pkAddr PublicKeyEvent
			payload, err := jsoncdc.Decode(nil, evt.Payload)
			if err != nil {
				log.Warn().Msgf("Could not decode payload %v %v", evt.Type, err.Error())
				continue
			}
			addEvent, ok := payload.(cadence.Event)
			if !ok {
				log.Warn().Msgf("could not decode event payload")
				continue
			}
			// field 0 is account address
			address := addEvent.Fields[0].String()
			// field 1 is public key
			pkValues, isOld := addEvent.Fields[1].(cadence.Array)
			if isOld {
				// old add account key format
				var dError error
				pkAddr, dError = CreatePublicKeyFromEvent(pkValues, address)
				if dError != nil {
					continue
				}
				if evt.Type == "flow.AccountKeyRemoved" {
					publicKeyActions.removes = append(publicKeyActions.removes, pkAddr)
				}
			}
			publicKeyActions.addresses = append(publicKeyActions.addresses, address)
		}
	}
	log.Debug().Msgf("total addrs %v, remove addrs %v", len(publicKeyActions.addresses), len(publicKeyActions.removes))
	return publicKeyActions, restartBulkLoad
}

func CreatePublicKeyFromEvent(cadenceArr cadence.Array, address string) (PublicKeyEvent, error) {
	pkBytes := ByteArrayValueToByteSlice(cadenceArr)
	publicKeyValue, decodeErr := flow.DecodeAccountKey(pkBytes)
	if decodeErr != nil {
		log.Warn().Msgf("could not decode public key %v", decodeErr.Error())
		return PublicKeyEvent{}, decodeErr
	}
	pk := Trim0x(publicKeyValue.PublicKey.String())
	pkAddr := PublicKeyEvent{pk, address}
	return pkAddr, nil
}

func ByteArrayValueToByteSlice(array cadence.Array) (result []byte) {
	result = make([]byte, 0, len(array.Values))
	for _, value := range array.Values {
		result = append(result, byte(value.(cadence.UInt8)))
	}
	return result
}

func Trim0x(hexString string) string {
	prefix := hexString[:2]
	if prefix == "0x" {
		return hexString[2:]
	}
	return hexString
}

func (fa *FlowAdapter) GetEventAddresses(flowClient *flowGrpc.Client, queries []EventRangeQuery) ([]PublicKeyActions, bool) {
	var allPkAddrs []PublicKeyActions
	restartBulkLoad := false
	var wg sync.WaitGroup
	eventRangeChan := make(chan EventRangeQuery)
	publicKeyChan := make(chan PublicKeyActions)

	go func() {
		for actions := range publicKeyChan {
			allPkAddrs = append(allPkAddrs, actions)
		}
	}()

	wg.Add(len(queries))
	go func() {
		for query := range eventRangeChan {
			log.Debug().Msgf("Query %v event blocks: %d   %d", query.Type, query.StartHeight, query.EndHeight)
			addrs, restart := RunAddressQuery(flowClient, fa.Context, query)
			if !restart {
				restartBulkLoad = restart
			}
			publicKeyChan <- addrs
			time.Sleep(500 * time.Millisecond) // give time for channel to process
			wg.Done()
		}
	}()

	for _, query := range queries {
		eventRangeChan <- query
	}

	wg.Wait()

	close(publicKeyChan)
	close(eventRangeChan)

	return allPkAddrs, restartBulkLoad
}

func splitQuery(query EventRangeQuery) []EventRangeQuery {
	rangef := float64(query.EndHeight - query.StartHeight)
	itemsPerRequest := math.Ceil(rangef / 2)
	return ChunkEventRangeQuery(int(itemsPerRequest), query.StartHeight, query.EndHeight, query.Type)
}
