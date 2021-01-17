package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

type Client struct {
	conn   net.Conn
	config *Config

	reader *bufio.Reader
	writer *bufio.Writer
}

func NewClient(config *Config) (*Client, error) {
	err := config.Validate()
	if err != nil {
		return nil, fmt.Errorf("Failed to create client: %s", err)
	}

	client := Client{}

	client.conn, err = net.Dial("tcp", config.ConnString)
	if err != nil {
		return nil, fmt.Errorf("Failed to create client: %s", err)
	}

	client.reader = bufio.NewReader(client.conn)
	client.writer = bufio.NewWriter(client.conn)

	return &client, nil
}

func (C *Client) StartShell() error {
	defer C.conn.Close()

	fmt.Println("Hariken shell version v0.1")
	for {
		fmt.Printf("$ ")

		done, err := C.Process(os.Stdin, os.Stdout)
		if err != nil {
			return err
		}

		if done {
			return nil
		}
	}
}

// Reads next command from `reader` and writes the output to `writer`
func (C *Client) Process(reader io.Reader, writer io.Writer) (bool, error) {
	bufferedReader := bufio.NewReader(reader)

	msg, err := bufferedReader.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("Failed to read string from stdin")
	}

	if len(strings.TrimSpace(msg)) == 0 {
		// Empty command so skip
		return false, nil
	}

	_, err = C.writer.WriteString(msg)
	if err != nil {
		return false, fmt.Errorf("Failed to write message to server")
	}

	err = C.writer.Flush()
	if err != nil {
		return false, err
	}

	reply, err := C.reader.ReadString('\n')
	if err != nil {
		return false, err
	}

	fmt.Fprint(writer, reply)
	if err != nil {
		return false, err
	}

	if reply == "KTHXBYE\n" {
		return true, nil
	}

	return false, nil
}
