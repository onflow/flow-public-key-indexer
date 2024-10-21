package addresses

import (
	"context"
	"os"
	"strconv"
	"time"

	"log"

	"github.com/joho/godotenv"
	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/access/grpc"
	"github.com/rs/zerolog"

	"example/flow-key-indexer/pkg/pg" // Update this path
)

func GetAddresses() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Set up logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create database configuration
	dbConfig := pg.DatabaseConfig{
		Host:     os.Getenv("KEYIDX_POSTGRESQLHOST"),
		Port:     mustAtoi(os.Getenv("KEYIDX_POSTGRESQLPORT")),
		User:     os.Getenv("KEYIDX_POSTGRESQLUSERNAME"),
		Password: os.Getenv("KEYIDX_POSTGRESQLPASSWORD"),
		Name:     os.Getenv("KEYIDX_POSTGRESQLDATABASE"),
	}

	// Create and start the database
	db := pg.NewStore(dbConfig, logger)
	err = db.Start(false) // false means don't purge on start
	if err != nil {
		log.Fatalf("Error starting database: %v", err)
	}

	// Set up AddressProvider
	accessNode := os.Getenv("KEYIDX_FLOWURL1")
	if accessNode == "" {
		log.Fatal("ACCESS_NODE_URL is not set in .env")
	}

	chainIDStr := os.Getenv("KEYIDX_CHAINID")
	if chainIDStr == "" {
		log.Fatal("KEYIDX_CHAINID is not set in .env")
	}
	chainID := flow.ChainID(chainIDStr)

	pauseDuration, err := time.ParseDuration("600ms")
	if err != nil {
		log.Fatalf("Invalid pause duration format: %v", err)
	}
	startIndex := uint(3000000)

	flowClient, err := grpc.NewClient(accessNode)
	if err != nil {
		log.Fatalf("Error creating Flow client: %v", err)
	}

	ctx := context.Background()
	addressProvider, err := NewAddressProvider(ctx, logger, chainID, flowClient, pauseDuration, startIndex)
	if err != nil {
		log.Fatalf("Error creating AddressProvider: %v", err)
	}

	// Generate batches of addresses
	batchSize := 1000
	batchChan := make(chan []flow.Address, 1000)
	go addressProvider.GenerateAddressBatches(ctx, batchChan, batchSize)
	// Process all batches
	for batch := range batchChan {
		// Convert addresses to strings
		addresses := make([]string, len(batch))
		for i, addr := range batch {
			addresses[i] = addr.HexWithPrefix()
		}

		logger.Info().Msgf("Storing %d addresses in the database", len(addresses))
		// Store addresses in the database
		err = db.StoreAddressesForProcessing(addresses)
		if err != nil {
			log.Fatalf("Error storing addresses in database: %v", err)
		}

		// Verify that addresses were stored
		// storedAddresses, err := db.GetAccountsToProcess(batchSize, nil)
		// require.NoError(t, err, "Error retrieving stored addresses")
		// assert.Len(t, storedAddresses, batchSize, "Number of stored addresses doesn't match batch size")

		// Optional: Add a break condition if you want to limit the number of batches processed
		// For example, to process only 5 batches:
		// if len(storedAddresses) >= 5*batchSize {
		// 	break
		// }
	}

	// Clean up (remove stored addresses)
	// err = db.RemoveAccountsProcessing(storedAddresses)
	// require.NoError(t, err, "Error removing stored addresses")
}

func mustAtoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return i
}
