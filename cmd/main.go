package main

import (
	"mai/internal/broker"
	http_server "mai/internal/http-server"
)

func main() {
	broker := broker.NewBroker()
	gs := http_server.NewGinServer(&broker)
	gs.Run()
}
