package server

import (
	"bufio"
	"fmt"
	"net"
	"testing"

	"github.com/oneshadab/hariken/pkg/protocol"
	"github.com/oneshadab/hariken/pkg/utils"
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
			command: fmt.Sprintf("GET %s", tData[0].tableName),
			expectedResult: utils.GenerateTable(
				[]string{"id"},
				[]map[string]string{}),
		},
		{
			command:        fmt.Sprintf("Insert %s name=%s", tData[0].tableName, tData[0].name),
			expectedResult: "OK",
		},
		{
			command: fmt.Sprintf("GET %s", tData[0].tableName),
			expectedResult: utils.GenerateTable(
				[]string{"id", "name"},
				[]map[string]string{{"id": tData[0].id, "name": tData[0].name}}),
		},
		{
			command: fmt.Sprintf("GET %s | FILTER id=%s | UPDATE age=%s", tData[0].tableName, tData[0].id, tData[0].age),
			expectedResult: utils.GenerateTable(
				[]string{"id", "name", "age"},
				[]map[string]string{{"id": tData[0].id, "name": tData[0].name, "age": tData[0].age}}),
		},
		{
			command: fmt.Sprintf("GET %s | FILTER id=%s", tData[0].tableName, tData[0].id),
			expectedResult: utils.GenerateTable(
				[]string{"id", "name", "age"},
				[]map[string]string{{"id": tData[0].id, "name": tData[0].name, "age": tData[0].age}})},
		{
			command:        fmt.Sprintf("USE %s", tData[1].databaseName),
			expectedResult: "OK",
		},
		{
			command: fmt.Sprintf("GET %s | FILTER id=%s", tData[1].tableName, tData[0].id),
			expectedResult: utils.GenerateTable(
				[]string{"id"},
				[]map[string]string{}),
		},
		{
			command:        fmt.Sprintf("Insert %s name=%s", tData[1].tableName, tData[1].name),
			expectedResult: "OK",
		},
		{
			command: fmt.Sprintf("GET %s", tData[1].tableName),
			expectedResult: utils.GenerateTable(
				[]string{"id", "name"},
				[]map[string]string{{"id": tData[1].id, "name": tData[1].name}})},
		{
			command:        fmt.Sprintf("GET %s | FILTER id=%s | DELETE", tData[1].tableName, tData[1].id),
			expectedResult: "OK",
		},
		{
			command: fmt.Sprintf("GET %s", tData[1].tableName),
			expectedResult: utils.GenerateTable(
				[]string{"id", "name"},
				[]map[string]string{}),
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
		ConnString:          "localhost:4253",
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
		err = protocol.WriteMessage(connWriter, tc.command)
		if err != nil {
			t.Fatal(err)
		}

		reply, err := protocol.ReadMessage(connReader)
		if err != nil {
			t.Fatal(err)
		}

		if reply != tc.expectedResult {
			t.Fatalf("TEST %d: Expected %s got %s for %s", i, tc.expectedResult, reply, tc.command)
		}
	}
}
