package client

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

type Client struct {
	conn   net.Conn
	config *Config
}

func NewClient(config *Config) (*Client, error) {
	var err error

	client := Client{}

	client.conn, err = net.Dial("tcp", config.ConnString)
	if err != nil {
		return nil, fmt.Errorf("Failed to create client: %s", err)
	}

	return &client, nil
}

func (client *Client) Shell() error {
	defer client.conn.Close()

	stdinReader := bufio.NewReader(os.Stdin)

	socketReader := bufio.NewReader(client.conn)
	socketWriter := bufio.NewWriter(client.conn)

	fmt.Println("Hariken shell version v0.1")

	for {
		fmt.Printf("$ ")

		msg, err := stdinReader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("Failed to read string from stdin")
		}

		_, err = socketWriter.WriteString(msg)
		if err != nil {
			return fmt.Errorf("Failed to write message to server")
		}

		err = socketWriter.Flush()
		if err != nil {
			return err
		}

		reply, err := socketReader.ReadString('\n')
		if err != nil {
			return err
		}

		fmt.Print(reply)

		if reply == "KTHXBYE\n" {
			return nil
		}
	}
}
