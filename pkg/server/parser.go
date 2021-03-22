package server

import (
	"fmt"
	"strings"

	"github.com/oneshadab/hariken/pkg/database"
)

func parseCommands(multiCmdStr string) ([]sessionCommand, error) {
	commands := make([]sessionCommand, 0)

	parts := strings.Split(multiCmdStr, "|")

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

func ExecCommand(query string, commandHandlers map[string]interface{}) (string, error) {
	ctx := &sessionCommandContext{
		tx:                    commandHandlers["startTransaction"].(func() *database.Transaction)(),
		ProcessedCommandTypes: make(map[string]bool),
	}

	defer ctx.tx.Cleanup()

	commands, err := parseCommands(query)
	if err != nil {
		return "", err
	}

	// Todo: Make commands in a chain atomic
	for _, cmd := range commands {

		handler, ok := sessionCommands[cmd.name]
		if !ok {
			return fmt.Sprintf("Command `%s` not found", cmd.name), nil
		}

		handler(ctx, cmd.args)

		ctx.ProcessedCommandTypes[cmd.name] = true
	}

	if ctx.ProcessedCommandTypes["USE"] ||
		ctx.ProcessedCommandTypes["INSERT"] ||
		ctx.ProcessedCommandTypes["DELETE"] {
		return ctx.resultOK()
	}

	return ctx.resultTable()
}
