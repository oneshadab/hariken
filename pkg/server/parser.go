package server

import (
	"strings"
)

func parseQuery(query string) ([]sessionCommand, error) {
	query = strings.TrimSuffix(query, "\n")

	commands := make([]sessionCommand, 0)

	parts := strings.Split(query, "|")

	for _, cmdStr := range parts {
		cmdStr = strings.TrimSpace(cmdStr)
		cmd := parseCommand(cmdStr)
		commands = append(commands, cmd)
	}

	return commands, nil
}

func parseCommand(cmdStr string) sessionCommand {
	parts := strings.Split(cmdStr, " ")

	return sessionCommand{
		name: strings.ToUpper(parts[0]),
		args: parts[1:],
	}
}
