package server

import (
	"strings"
)

func parseQuery(queryStr string) (Query, error) {
	queryStr = strings.TrimSuffix(queryStr, "\n")

	commands := make([]QueryCommand, 0)
	parts := strings.Split(queryStr, "|")

	for _, cmdStr := range parts {
		cmdStr = strings.TrimSpace(cmdStr)
		cmd := parseCommand(cmdStr)
		commands = append(commands, cmd)
	}

	q := Query{
		commands: commands,
	}

	return q, nil
}

func parseCommand(cmdStr string) QueryCommand {
	parts := strings.Split(cmdStr, " ")

	return QueryCommand{
		name: strings.ToUpper(parts[0]),
		args: parts[1:],
	}
}
