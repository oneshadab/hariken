package server

import (
	"strings"
)

func parseQuery(query string) ([]sesCommand, error) {
	query = strings.TrimSuffix(query, "\n")

	commands := make([]sesCommand, 0)

	parts := strings.Split(query, "|")

	for _, cmdStr := range parts {
		cmdStr = strings.TrimSpace(cmdStr)
		cmd := parseCommand(cmdStr)
		commands = append(commands, cmd)
	}

	return commands, nil
}

func parseCommand(cmdStr string) sesCommand {
	parts := strings.Split(cmdStr, " ")

	return sesCommand{
		name: strings.ToUpper(parts[0]),
		args: parts[1:],
	}
}
