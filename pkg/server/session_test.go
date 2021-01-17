package server

import (
	"bufio"
	"bytes"
	"testing"
)

type TestCase struct {
	command        string
	expectedResult string
}

func TestCommands(t *testing.T) {
	testCases := []TestCase{
		{
			command:        "HAS name",
			expectedResult: "False",
		},
		{
			command:        "SET name john",
			expectedResult: "OK",
		},
		{
			command:        "HAS name",
			expectedResult: "True",
		},
		{
			command:        "GET name",
			expectedResult: `"john"`,
		},
		{
			command:        "USE test",
			expectedResult: "OK",
		},
		{
			command:        "HAS name",
			expectedResult: "False",
		},
		{
			command:        "SET name jack",
			expectedResult: "OK",
		},
		{
			command:        "DELETE name",
			expectedResult: "OK",
		},
		{
			command:        "GET name",
			expectedResult: "nil",
		},
	}

	config := &Config{
		ConnString:       "localhost:4252",
		StorageRoot:      t.TempDir(),
		DefaultStoreName: "default",
	}

	var input, output bytes.Buffer
	reader := bufio.NewReader(&input)
	writer := bufio.NewWriter(&output)

	session, err := NewSession(reader, writer, config)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range testCases {
		result, err := session.Exec(tc.command)
		if err != nil {
			t.Fatal(err)
		}

		if result != tc.expectedResult {
			t.Fatalf("Expected `%s` got `%s`, for command `%s`", tc.expectedResult, result, tc.command)
		}

	}
}
