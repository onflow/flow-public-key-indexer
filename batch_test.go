package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/onflow/flow-go-sdk/access/grpc"

	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestBatchAddresses(t *testing.T) {
	ctx := context.Background()
	conf := DefaultConfig
	addressChan := make(chan []flow.Address)
	// test against mainnet
	c := NewFlowClient("access.mainnet.nodes.onflow.org:9000")
	// Set up zerolog to log to the console

	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	go func() {
		defer close(addressChan)
		h, err := GetAllAddresses(c.Client, ctx, logger, conf, addressChan)
		if err != nil {
			fmt.Printf("Error getting addresses: %v\n", err)
		}
		fmt.Printf("Got addresses up to block height %d\n", h)
	}()

	for accountAddresses := range addressChan {
		// log out information about the account
		fmt.Printf("Checking address %s\n", accountAddresses)
	}
}

func TestSubscribingToEvents(t *testing.T) {
	ctx := context.Background()
	flowClient, err := grpc.NewClient("access.testnet.nodes.onflow.org:9000")

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Flow client")
	}

	header, err := flowClient.GetLatestBlockHeader(ctx, true)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get latest block header")
	}

	fmt.Printf("Block Height: %d\n", header.Height)
	fmt.Printf("Block ID: %s\n", header.ID)

	data, errChan, initErr := flowClient.SubscribeEventsByBlockID(ctx, header.ID, flow.EventFilter{})
	if initErr != nil {

		log.Fatal().Err(initErr).Msg("Failed to subscribe to events")
	}
	for {
		select {
		case <-ctx.Done():
			return
		case eventData, ok := <-data:
			if !ok {
				panic("data subscription closed")
			}
			fmt.Printf("~~~ Height: %d ~~~\n", eventData.Height)
			printEvents(eventData.Events)
		case err, ok := <-errChan:
			if !ok {
				panic("error channel subscription closed")
			}
			fmt.Printf("~~~ ERROR: %s ~~~\n", err.Error())
		}
	}
}

func printEvents(events []flow.Event) {
	for _, event := range events {
		fmt.Printf("\n\nType: %s", event.Type)
		fmt.Printf("\nValues: %v", event.Value)
		fmt.Printf("\nTransaction ID: %s", event.TransactionID)
	}
}
