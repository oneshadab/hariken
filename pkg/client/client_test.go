package client

import (
	"bufio"
	"bytes"
	"net"
	"testing"

	"github.com/oneshadab/hariken/pkg/protocol"
)

func TestClient(t *testing.T) {
	testCases := []struct {
		message       string
		expectedReply string
	}{
		{
			message:       "GET name",
			expectedReply: "GET name",
		},
		{
			message:       "KTHXBYE",
			expectedReply: "KTHXBYE",
		},
	}

	config := &Config{
		ConnString: "localhost:4252",
	}

	go func() {
		err := newEchoServer(config.ConnString)
		if err != nil {
			t.Fatal(err)
		}
	}()

	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}

	for i, tc := range testCases {
		reader := bytes.NewBufferString(tc.message + "\n")
		writer := bytes.NewBuffer(nil)

		done, err := client.Process(reader, writer)
		if err != nil {
			t.Fatal(err)
		}

		reply := writer.String()
		if reply != tc.expectedReply+"\n" {
			t.Fatalf("TEST %d: Expected `%s` got `%s` for `%s`", i, tc.expectedReply, reply, tc.message)
		}

		if done {
			break
		}
	}

}

func newEchoServer(connString string) error {
	listener, err := net.Listen("tcp", connString)
	if err != nil {
		return err
	}

	conn, err := listener.Accept()
	if err != nil {
		return err
	}

	connReader := bufio.NewReader(conn)
	connWriter := bufio.NewWriter(conn)

	for {
		msg, err := protocol.ReadMessage(connReader)
		if err != nil {
			return err
		}

		err = protocol.WriteMessage(connWriter, msg)
		if err != nil {
			return err
		}
	}
}
