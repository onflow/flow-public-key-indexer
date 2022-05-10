package main

import (
	"encoding/json"
	"fmt"
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
	err := envconfig.Process("keyIdx", &p)

	if err != nil {
		logger.Fatal(err.Error())
	}
	format, _ := json.MarshalIndent(p, "", "    ")
	fmt.Printf("%+v\n", string(format))
	lvl, err := zerolog.ParseLevel(p.LogLevel)
	if err == nil {
		zerolog.SetGlobalLevel(lvl)
		log.Info().Msgf("Set log level to %s", lvl.String())
	}

	a.Initialize(p)

	a.Run()

}
