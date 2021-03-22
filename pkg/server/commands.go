package server

import (
	"fmt"
	"strings"

	"github.com/oneshadab/hariken/pkg/database"
	"github.com/oneshadab/hariken/pkg/utils"
)

type sesCommand struct {
	name string
	args []string
}

type sesCommandHandler func(ctx *sesCommandContext, args []string) (result string, err error)
type sesCommandContext struct {
	ses           *Session
	db            *database.Database
	tx            *database.Transaction
	err           *error
	processedCmds map[string]bool
}

var availableCommands = map[string]sesCommandHandler{
	"USE":    useCmd,
	"INSERT": insertCmd,
	"GET":    getCmd,
	"DELETE": deleteCmd,
	"UPDATE": updateCmd,
	"FILTER": filterCmd,
	"EXIT":   exitCmd,
}

func useCmd(ctx *sesCommandContext, args []string) (string, error) {
	if len(args) > 1 || len(ctx.processedCmds) > 0 {
		return fmt.Sprintf("Invalid syntax for `use`"), nil
	}

	dbName := args[0]

	err := ctx.ses.useDatabase(dbName)

	if err != nil {
		return "", err
	}

	return ctx.resultOK()
}

func insertCmd(ctx *sesCommandContext, args []string) (string, error) {
	if len(args) <= 1 {
		return fmt.Sprintf("Invalid syntax for `insert`"), nil
	}

	tableName := args[0]
	entries := make(map[string]string)
	for _, entry := range args[1:] {
		parts := strings.Split(entry, "=")
		key := parts[0]
		val := parts[1]

		entries[key] = val
	}

	ctx.tx.UseTable(tableName)
	ctx.tx.InsertRow(entries)

	return ctx.resultOK()
}

func getCmd(ctx *sesCommandContext, args []string) (string, error) {
	if len(args) > 1 {
		return fmt.Sprintf("Invalid syntax for `get`"), nil
	}

	tableName := args[0]
	ctx.tx.UseTable(tableName)
	ctx.tx.FetchAll()

	return ctx.resultTable()
}

func deleteCmd(ctx *sesCommandContext, args []string) (string, error) {
	for _, row := range ctx.tx.Result {
		ctx.tx.DeleteRow(row.Id())
	}

	return ctx.resultOK()
}

func updateCmd(ctx *sesCommandContext, args []string) (string, error) {
	if len(args) == 0 {
		return fmt.Sprintf("Invalid syntax for `update`"), nil
	}

	entries := make(map[string]string)
	for _, entry := range args {
		parts := strings.Split(entry, "=")
		key := parts[0]
		val := parts[1]
		entries[key] = val

		ctx.tx.UpdateAll(entries)
	}

	return ctx.resultTable()
}

func filterCmd(ctx *sesCommandContext, args []string) (string, error) {
	if len(args) == 0 {
		return fmt.Sprintf("Invalid syntax for `filter`"), nil
	}

	for _, entry := range args {
		parts := strings.Split(entry, "=")
		key := parts[0]
		val := parts[1]

		ctx.tx.Filter(key, val)
	}

	return ctx.resultTable()
}

func exitCmd(ctx *sesCommandContext, args []string) (string, error) {
	return "KTHXBYE", nil
}

func (ctx *sesCommandContext) result() (string, error) {
	hasUsedShortResultCmd := false
	for _, shortResultCmd := range []string{"USE", "INSERT", "DELETE"} {
		hasUsedShortResultCmd = hasUsedShortResultCmd || ctx.processedCmds[shortResultCmd]
	}

	if hasUsedShortResultCmd {
		return ctx.resultOK()
	}
	return ctx.resultTable()
}

func (ctx *sesCommandContext) resultOK() (string, error) {
	return "OK", nil
}

func (ctx *sesCommandContext) resultTable() (string, error) {
	headers, err := ctx.tx.Table.Columns()
	if err != nil {
		return "", err
	}

	result := []map[string]string{}
	for _, v := range ctx.tx.Result {
		result = append(result, v.Column)
	}

	output := utils.GenerateTable(headers, result)
	return output, nil
}
