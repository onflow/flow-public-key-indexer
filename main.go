package main

import (
	"flag"
	"fmt"

	"github.com/axiomzen/envconfig"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"example/flow-key-indexer/cmd/addresses"
)

func main() {
	//  go run . -get-addresses
	getAddressesFlag := flag.Bool("get-addresses", false, "Run GetAddresses function")
	flag.Parse()

	if *getAddressesFlag {
		fmt.Println("Running GetAddresses...")
		addresses.GetAddresses()
		return
	}

	a := App{}

	if err := godotenv.Load("./.env"); err != nil {
		log.Fatal().Err(err).Msg("Error loading .env file")
	}

	var p Params
	err := envconfig.Process("KEYIDX", &p)
	if err != nil {
		log.Fatal().Err(err).Msg("Error processing environment variables")
	}

	lvl, err := zerolog.ParseLevel(p.LogLevel)
	if err == nil {
		zerolog.SetGlobalLevel(lvl)
		log.Info().Msgf("Set log level to %s", lvl.String())
	}

	a.Initialize(p)
	a.Run()
}
