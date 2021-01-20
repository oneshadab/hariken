package client

import (
	"bytes"
	"net"
	"testing"
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

	go newEchoServer(config.ConnString)

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
			t.Fatalf("TEST %d: Expected %s got %s for %s", i, tc.expectedReply, reply, tc.message)
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

	for {
		data := make([]byte, 1)

		_, err = conn.Read(data)
		if err != nil {
			return err
		}

		_, err = conn.Write(data)
		if err != nil {
			return err
		}
	}
}
