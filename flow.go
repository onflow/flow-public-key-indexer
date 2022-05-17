package main

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/client"
	"google.golang.org/grpc"
)

type FlowAdapter struct {
	Client  *client.Client
	Context context.Context
	URL     string
}

func NewFlowClient(url string) *FlowAdapter {
	adapter := FlowAdapter{}
	adapter.Context = context.Background()
	// any reason to pass this as an arg instead?
	adapter.URL = url

	// create flow client
	FlowClient, err := client.New(strings.TrimSpace(adapter.URL), grpc.WithInsecure())
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

func (fa *FlowAdapter) GetAddressesFromBlockEvents(flowUrls []string, startBlockheight uint64, maxBlockRange int, waitNumBlocks int) ([]flow.Address, uint64, bool) {
	itemsPerRequest := 245 // access node can only handel 250
	eventTypes := []string{"flow.AccountKeyAdded", "flow.AccountKeyRemoved"}
	var addresses []flow.Address
	restartBulkLoad := true
	currentHeight, err := fa.GetCurrentBlockHeight()
	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
		return addresses, currentHeight, restartBulkLoad
	}

	// backing off a few blocks to allow for buffer
	backOfHeight := currentHeight // - 10
	// create chunks to make sure query limit is not exceeded
	var chunksEvents []client.EventRangeQuery

	for _, eventType := range eventTypes {
		events := ChunkEventRangeQuery(itemsPerRequest, startBlockheight, backOfHeight, eventType)
		chunksEvents = append(chunksEvents, events...)
	}

	addrs, restartDataLoader := fa.GetEventAddresses(flowUrls, chunksEvents)
	return unique(addrs), backOfHeight, restartDataLoader
}

func ChunkEventRangeQuery(itemsPerRequest int, lowBlockHeight, highBlockHeight uint64, eventName string) []client.EventRangeQuery {
	maxServerBlockRange := float64(itemsPerRequest) // less than server's max of 250
	var chunks []client.EventRangeQuery
	totalRange := float64(highBlockHeight - lowBlockHeight)
	numChunks := math.Ceil(totalRange / maxServerBlockRange)
	lowHeight := lowBlockHeight
	highHeight := uint64(lowHeight + uint64(maxServerBlockRange))
	for i := 0; i < int(numChunks); i++ {
		query := client.EventRangeQuery{
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

type AccountKeyAdded struct {
	Type  string `json:"type"`
	Value struct {
		ID     string `json:"id"`
		Fields []struct {
			Name  string `json:"name"`
			Value struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			} `json:"value"`
		} `json:"fields"`
	} `json:"value"`
}

func RunAddressQuery(client *client.Client, context context.Context, query client.EventRangeQuery) ([]flow.Address, bool) {
	var addresses []flow.Address
	restartBulkLoad := false
	events, err := client.GetEventsForHeightRange(context, query)
	if err != nil {
		log.Warn().Err(err).Msgf("retrying get events in block range %d %d", query.StartHeight, query.EndHeight)
		// break up query into smaller chunks
		for _, q := range splitQuery(query) {
			eventsRetry, errRetry := client.GetEventsForHeightRange(context, q)
			if errRetry != nil {
				log.Error().Err(errRetry).Msg("retrying get events failed")
				return addresses, true
			}
			events = append(events, eventsRetry...)
		}
	}
	for _, event := range events {
		for _, evt := range event.Events {
			var data AccountKeyAdded
			json.Unmarshal([]byte(evt.Payload), &data)
			for _, field := range data.Value.Fields {
				if field.Name == "address" {
					addresses = append(addresses, flow.HexToAddress(field.Value.Value))
				}
			}
		}
	}
	return addresses, restartBulkLoad
}

func (fa *FlowAdapter) GetEventAddresses(flowUrls []string, queries []client.EventRangeQuery) ([]flow.Address, bool) {
	var allAddresses []flow.Address
	restartBulkLoad := false
	var wg sync.WaitGroup
	eventRangeChan := make(chan client.EventRangeQuery)
	addressChan := make(chan []flow.Address)

	go func() {
		for address := range addressChan {
			allAddresses = append(allAddresses, address...)
		}
	}()

	wg.Add(len(queries))
	for i := 0; i < len(flowUrls); i++ {
		flowUrl := flowUrls[i]
		go func() {
			// Each worker has a separate Flow client
			client := getFlowClient(flowUrl)
			defer func() {
				err := client.Close()
				if err != nil {
					log.Warn().
						Err(err).
						Msg("error closing client")
				}
			}()

			for query := range eventRangeChan {
				log.Debug().Msgf("Query %v event blocks: %d   %d", query.Type, query.StartHeight, query.EndHeight)
				addrs, restart := RunAddressQuery(client, fa.Context, query)
				if !restart {
					restartBulkLoad = restart
				}
				addressChan <- addrs
				wg.Done()
			}
		}()
	}

	for _, query := range queries {
		eventRangeChan <- query
	}

	wg.Wait()

	close(addressChan)
	close(eventRangeChan)

	return allAddresses, restartBulkLoad
}

func splitQuery(query client.EventRangeQuery) []client.EventRangeQuery {
	rangef := float64(query.EndHeight - query.StartHeight)
	itemsPerRequest := math.Ceil(rangef / 2)
	return ChunkEventRangeQuery(int(itemsPerRequest), query.StartHeight, query.EndHeight, query.Type)
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
