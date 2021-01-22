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
		tableName    string

		id   string
		name string
		age  string
	}{
		{
			databaseName: "default",
			tableName:    "users",

			id:   "0",
			name: "john",
			age:  "32",
		},
		{
			databaseName: "test",
			tableName:    "users",

			id:   "0",
			name: "jack",
		},
	}

	testCases := []struct {
		command        string
		expectedResult string
	}{
		{
			command:        fmt.Sprintf("GET %s %s", tData[0].tableName, tData[0].id),
			expectedResult: "nil",
		},
		{
			command:        fmt.Sprintf("UPSERT %s name=%s", tData[0].tableName, tData[0].name),
			expectedResult: `{"Column":{"id":"0","name":"john"}}`,
		},
		{
			command:        fmt.Sprintf("GET %s %s", tData[0].tableName, tData[0].id),
			expectedResult: `{"Column":{"id":"0","name":"john"}}`,
		},
		{
			command:        fmt.Sprintf("UPSERT %s id=%s age=%s", tData[0].tableName, tData[0].id, tData[0].age),
			expectedResult: `{"Column":{"age":"32","id":"0","name":"john"}}`,
		},
		{
			command:        fmt.Sprintf("GET %s %s", tData[0].tableName, tData[0].id),
			expectedResult: `{"Column":{"age":"32","id":"0","name":"john"}}`,
		},
		{
			command:        fmt.Sprintf("USE %s", tData[1].databaseName),
			expectedResult: "OK",
		},
		{
			command:        fmt.Sprintf("GET %s %s", tData[1].tableName, tData[0].id),
			expectedResult: "nil",
		},
		{
			command:        fmt.Sprintf("Upsert %s name=%s", tData[1].tableName, tData[1].name),
			expectedResult: `{"Column":{"id":"0","name":"jack"}}`,
		},
		{
			command:        fmt.Sprintf("GET %s %s", tData[1].tableName, tData[1].id),
			expectedResult: `{"Column":{"id":"0","name":"jack"}}`,
		},
		{
			command:        fmt.Sprintf("DELETE %s %s", tData[1].tableName, tData[1].id),
			expectedResult: "OK",
		},
		{
			command:        fmt.Sprintf("GET %s %s", tData[1].tableName, tData[1].id),
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
