package main

import (
	"github.com/new-kafka/broker/internal/broker"
	http_server "github.com/new-kafka/broker/internal/http-server"
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
	b := broker.NewBroker()
	gs := http_server.NewGinServer(b)
	gs.Run()
}
