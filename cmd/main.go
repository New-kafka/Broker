package main

import (
	"github.com/new-kafka/broker/internal/broker"
	http_server "github.com/new-kafka/broker/internal/http-server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func init() {
	viper.SetConfigName("config")
	viper.AddConfigPath("./config")

	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
}

func main() {
	// TODO: get log level from .env or config
	log.SetLevel(log.DebugLevel)
	b := broker.NewBroker()
	gs := http_server.NewGinServer(b)
	gs.Run()
}
