package main

import (
	"github.com/spf13/viper"
	"mai/internal/broker"
	http_server "mai/internal/http-server"
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
