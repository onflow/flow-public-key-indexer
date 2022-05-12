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

func (fa *FlowAdapter) GetAddressesFromBlockEvents(concurrenClients int, startBlockheight uint64, maxBlockRange int, waitNumBlocks int) ([]flow.Address, uint64, bool) {
	itemsPerRequest := 240 // access node can only handel 250
	eventTypes := []string{"flow.AccountKeyAdded", "flow.AccountKeyRemoved"}
	var addresses []flow.Address
	restartBulkLoad := true
	currentHeight, err := fa.GetCurrentBlockHeight()
	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
		return addresses, currentHeight, restartBulkLoad
	}

	// create chunks to make sure query limit is not exceeded
	var chunksEvents []client.EventRangeQuery

	for _, eventType := range eventTypes {
		events := ChunkEventRangeQuery(itemsPerRequest, startBlockheight, currentHeight, eventType)
		chunksEvents = append(chunksEvents, events...)
	}

	addrs, restartDataLoader := fa.GetEventAddresses(concurrenClients, chunksEvents)
	return unique(addrs), currentHeight, restartDataLoader
}

func ChunkEventRangeQuery(size int, lowBlockHeight, highBlockHeight uint64, eventName string) []client.EventRangeQuery {
	maxServerBlockRange := float64(size) // less than server's max of 250
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
	log.Debug().Msgf("query %v", query)
	events, err := client.GetEventsForHeightRange(context, query)
	if err != nil {
		log.Error().Err(err).Msgf("Could not get events in block range %d", query.EndHeight-query.StartHeight)
		return addresses, true
	}
	for _, event := range events {
		for _, evt := range event.Events {
			var data AccountKeyAdded
			json.Unmarshal([]byte(evt.Payload), &data)
			for _, field := range data.Value.Fields {
				if field.Name == "address" {
					log.Debug().Msgf("xxx address: %s", field.Value.Value)
					addresses = append(addresses, flow.HexToAddress(field.Value.Value))
				}
			}
		}
	}
	log.Debug().Msgf("num addresses %v", len(addresses))
	return addresses, restartBulkLoad
}

func (fa *FlowAdapter) GetEventAddresses(maxClients int, queries []client.EventRangeQuery) ([]flow.Address, bool) {
	var allAddresses []flow.Address
	restartBulkLoad := false
	var wg sync.WaitGroup
	eventRangeChan := make(chan client.EventRangeQuery)
	addressChan := make(chan []flow.Address)

	go func() {
		for address := range addressChan {
			log.Debug().Msgf("have addrs to add to all address %d", len(address))
			allAddresses = append(allAddresses, address...)
			log.Debug().Msgf("addresses count in all addresses %d", len(allAddresses))
		}
	}()

	wg.Add(len(queries))
	for i := 0; i < maxClients; i++ {
		go func() {
			// Each worker has a separate Flow client
			client := getFlowClient(fa.URL)
			defer func() {
				err := client.Close()
				if err != nil {
					log.Warn().
						Err(err).
						Msg("error closing client")
				}
			}()

			for query := range eventRangeChan {
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
