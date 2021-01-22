package server

import (
	"bufio"
	"fmt"
	"net"
	"testing"
)

func TestServer(t *testing.T) {
	tData := []struct {
		databaseName string
		key          string
		value        string
	}{
		{
			databaseName: "default",
			key:          "name",
			value:        "john",
		},
		{
			databaseName: "test",
			key:          "name",
			value:        "jack",
		},
	}

	testCases := []struct {
		command        string
		expectedResult string
	}{
		{
			command:        fmt.Sprintf("GET %s", tData[0].key),
			expectedResult: "nil",
		},
		{
			command:        fmt.Sprintf("SET %s %s", tData[0].key, tData[0].value),
			expectedResult: "OK",
		},
		{
			command:        fmt.Sprintf("HAS %s", tData[0].key),
			expectedResult: "True",
		},
		{
			command:        fmt.Sprintf("GET %s", tData[0].key),
			expectedResult: `"john"`,
		},
		{
			command:        fmt.Sprintf("USE %s", tData[1].databaseName),
			expectedResult: "OK",
		},
		{
			command:        fmt.Sprintf("HAS %s", tData[0].key),
			expectedResult: "False",
		},
		{
			command:        fmt.Sprintf("HAS %s", tData[1].key),
			expectedResult: "False",
		},
		{
			command:        fmt.Sprintf("SET %s %s", tData[1].key, tData[1].value),
			expectedResult: "OK",
		},
		{
			command:        fmt.Sprintf("GET %s", tData[1].key),
			expectedResult: `"jack"`,
		},
		{
			command:        fmt.Sprintf("DELETE %s", tData[1].key),
			expectedResult: "OK",
		},
		{
			command:        fmt.Sprintf("GET %s", tData[1].key),
			expectedResult: "nil",
		},
		{
			command:        "INVALIDCOMMMAND",
			expectedResult: "Command `INVALIDCOMMMAND` not found",
		},
		{
			command:        "exit",
			expectedResult: "KTHXBYE",
		},
	}

	config := &Config{
		ConnString:          "localhost:4252",
		StorageRoot:         t.TempDir(),
		DefaultDatabaseName: "default",
	}

	server, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}

	go server.WaitForConnections()

	conn, err := net.Dial("tcp", config.ConnString)
	if err != nil {
		t.Fatal(err)
	}

	connReader := bufio.NewReader(conn)
	connWriter := bufio.NewWriter(conn)

	for i, tc := range testCases {
		_, err = fmt.Fprintln(connWriter, tc.command)
		if err != nil {
			t.Fatal(err)
		}
		err = connWriter.Flush()
		if err != nil {
			t.Fatal(err)
		}

		reply, err := connReader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}

		if reply != tc.expectedResult+"\n" {
			t.Fatalf("TEST %d: Expected %s got %s for %s", i, tc.expectedResult, reply, tc.command)
		}
	}
}
