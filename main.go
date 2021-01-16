package main

import (
	"os"

	"github.com/oneshadab/hariken/pkg/client"
	"github.com/oneshadab/hariken/pkg/server"
)

const (
	connString = "localhost:6768"
)

func main() {
	cmd := "startServerAndConnect"
	if len(os.Args) >= 2 {
		cmd = os.Args[1]
	}

	if cmd == "connect" {
		connect()
	}

	if cmd == "startServer" {
		startServer()
	}

	if cmd == "startServerAndConnect" {
		startServerAsync()
		connect()
	}
}

func connect() {
	client, err := client.NewClient(clientConfig())
	if err != nil {
		panic(err)
	}
	client.Shell()
}

func startServer() {
	server, err := server.NewServer(serverConfig())
	if err != nil {
		panic(err)
	}
	server.WaitForConnections()
}

func startServerAsync() {
	server, err := server.NewServer(serverConfig())
	if err != nil {
		panic(err)
	}
	go server.WaitForConnections()
}

func serverConfig() *server.Config {
	return &server.Config{
		ConnString:       connString,
		StorageRoot:      "temp/db",
		DefaultStoreName: "default",
	}
}

func clientConfig() *client.Config {
	return &client.Config{
		ConnString: connString,
	}
}
