package main

import (
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

	a.Run()

}
