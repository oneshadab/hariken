package main

import (
	"github.com/oneshadab/hariken/pkg/client"
	"github.com/oneshadab/hariken/pkg/server"
)

func main() {
	connString := "localhost:6768"

	server, err := server.NewServer(connString)
	if err != nil {
		panic(err)
	}
	go server.Listen()

	client, err := client.NewClient(connString)
	if err != nil {
		panic(err)
	}
	client.Shell()
}
