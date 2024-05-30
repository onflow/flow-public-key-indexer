package main

import (
	"context"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/onflow/cadence"
	jsoncdc "github.com/onflow/cadence/encoding/json"
	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
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
			grpc.MaxCallRecvMsgSize(16*1024*1024), // 16 MB
			grpc.MaxCallSendMsgSize(16*1024*1024), // 16 MB
		),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)), // Optional: Use compression
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

func (fa *FlowAdapter) GetAddressesFromBlockEvents(flowUrls []string, startBlockHeight uint64) ([]string, uint64, error) {
	eventTypes := []string{"flow.AccountKeyAdded", "flow.AccountKeyRemoved"}
	currentHeight, err := fa.GetCurrentBlockHeight()

	if err != nil {
		log.Error().Err(err).Msg("Could not get current block height")
		return []string{}, currentHeight, err
	}

	BlockHeight := currentHeight
	var queryEvents []client.EventRangeQuery

	for _, eventType := range eventTypes {
		queryEvents = append(queryEvents, client.EventRangeQuery{
			Type:        eventType,
			StartHeight: startBlockHeight,
			EndHeight:   BlockHeight,
		})
	}

	addrs, err := fa.GetEventAddresses(flowUrls, queryEvents)
	if err != nil {
		log.Error().Err(err).Msg("Could not get event addresses")
		return addrs, BlockHeight, err
	}

	return addrs, BlockHeight, nil
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
			log.Debug().Msgf("event type %v", evt.Type)
			var pkAddr PublicKeyEvent
			payload, err := jsoncdc.Decode(evt.Payload)
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
				address = addEvent.Fields[0].String()
				allAccountAddresses = append(allAccountAddresses, address)
			}
			if evt.Type == "flow.AccountKeyRemoved" {
				allAccountAddresses = append(allAccountAddresses, pkAddr.account)
			}
		}
	}
	log.Debug().Msgf("total addrs %v affected", len(allAccountAddresses))
	return allAccountAddresses, nil
}
func (fa *FlowAdapter) GetEventAddresses(flowUrls []string, queries []client.EventRangeQuery) ([]string, error) {
	var allPkAddrs []string
	var callingError error
	eventRangeChan := make(chan client.EventRangeQuery)
	publicKeyChan := make(chan string)
	errChan := make(chan error, len(queries)) // buffered channel to handle errors

	var wg sync.WaitGroup
	wg.Add(len(flowUrls))

	go func() {
		for addrs := range publicKeyChan {
			allPkAddrs = append(allPkAddrs, addrs)
		}
	}()

	go func() {
		wg.Wait()
		close(publicKeyChan)
		close(errChan)
	}()

	for i := 0; i < len(flowUrls); i++ {
		flowUrl := flowUrls[i]
		go func(flowUrl string) {
			defer wg.Done()

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
				log.Debug().Msgf("Query %v event blocks: %d %d, range %d", query.Type, query.StartHeight, query.EndHeight, query.EndHeight-query.StartHeight)

				addrs, err := RunAddressQuery(client, fa.Context, query)
				if err != nil {
					log.Error().Err(err).Msg("Error getting event addresses, sending error to error channel")
					errChan <- err // send error to error channel
					return
				}

				for _, addr := range addrs {
					publicKeyChan <- addr
				}
			}
		}(flowUrl)
	}

	go func() {
		for _, query := range queries {
			eventRangeChan <- query
		}
		close(eventRangeChan)
	}()

	for err := range errChan {
		if err != nil {
			callingError = err
			break
		}
	}

	log.Debug().Msgf("Total addresses %v  has error: %v", len(allPkAddrs), callingError == nil)
	return allPkAddrs, callingError
}
