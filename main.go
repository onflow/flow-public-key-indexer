package main

import (
	"flag"
	logger "log"

	"github.com/axiomzen/envconfig"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	a := App{}

	if err := godotenv.Load("./.env"); err != nil {
		logger.Fatal("error loading godotenv")
	}
	var p Params
	err := envconfig.Process("KEYIDX", &p)

	if err != nil {
		logger.Fatal(err.Error())
	}
	lvl, err := zerolog.ParseLevel(p.LogLevel)
	if err == nil {
		zerolog.SetGlobalLevel(lvl)
		log.Info().Msgf("Set log level to %s", lvl.String())
	}

	a.Initialize(p)

	runBackfill := flag.Bool("backfill", false, "Run backfill process")
	flag.Parse()

	if *runBackfill {
		log.Debug().Msg("Running backfill")
		if err := backfillPublicKeys(a.DB, a.flowClient); err != nil {
			log.Fatal().Err(err).Msg("Backfill failed")
		}
		return
	}

	a.Run()

}
