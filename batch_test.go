package main

import (
	"context"
	"fmt"
	"os"
	"testing"

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
