package main

import (
	"context"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/onflow/cadence"
	jsoncdc "github.com/onflow/cadence/encoding/json"
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
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(20 * 1024 * 1024), // 16 MB
		),
		// grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)), // Optional: Use compression
	}
	// create flow client
	FlowClient, err := client.New(strings.TrimSpace(adapter.URL), opts...)
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

func (fa *FlowAdapter) GetAddressesFromBlockEvents(flowUrls []string, startBlockHeight uint64, endBlockHeight uint64) ([]string, uint64, error) {
	eventTypes := []string{"flow.AccountKeyAdded", "flow.AccountKeyRemoved"}

	var queryEvents []client.EventRangeQuery

	for _, eventType := range eventTypes {
		queryEvents = append(queryEvents, client.EventRangeQuery{
			Type:        eventType,
			StartHeight: startBlockHeight,
			EndHeight:   endBlockHeight,
		})
	}

	addrs, err := fa.GetEventAddresses(flowUrls, queryEvents)
	if err != nil {
		log.Error().Err(err).Msg("Could not get event addresses")
		return addrs, endBlockHeight, err
	}

	return addrs, endBlockHeight, nil
}

func RunAddressQuery(client *client.Client, context context.Context, query client.EventRangeQuery) ([]string, error) {
	var allAccountAddresses []string
	events, err := client.GetEventsForHeightRange(context, query)
	log.Debug().Msgf("events %v", len(events))
	if err != nil {
		log.Warn().Err(err).Msgf("Error events in block range %d %d", query.StartHeight, query.EndHeight)
		return allAccountAddresses, err
	}
	for _, event := range events {
		for _, evt := range event.Events {
			var pkAddr string
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
			var address string
			if evt.Type == "flow.AccountKeyAdded" {
				address = addEvent.FieldsMappedByName()["address"].(cadence.Address).String()
				allAccountAddresses = append(allAccountAddresses, address)
			}
			if evt.Type == "flow.AccountKeyRemoved" {
				allAccountAddresses = append(allAccountAddresses, pkAddr)
			}
		}
	}
	return allAccountAddresses, nil
}

func (fa *FlowAdapter) GetEventAddresses(flowUrls []string, queries []client.EventRangeQuery) ([]string, error) {
	allPkAddrs := []string{} // Initialize the slice directly

	// Use the first URL to create a single client
	client := getFlowClient(flowUrls[0])
	defer client.Close()

	for _, query := range queries {
		log.Debug().Msgf("Querying %v event blocks: %d %d, range %d", query.Type, query.StartHeight, query.EndHeight, query.EndHeight-query.StartHeight)

		addrs, err := RunAddressQuery(client, fa.Context, query)
		if err != nil {
			log.Error().Err(err).Msg("Error getting event addresses")
			return allPkAddrs, err // Return the error immediately with processed addresses
		}

		allPkAddrs = append(allPkAddrs, addrs...)
	}

	log.Debug().Msgf("Flow: Event Found Total addresses: %d", len(allPkAddrs))
	return allPkAddrs, nil
}
