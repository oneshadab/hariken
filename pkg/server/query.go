package server

import (
	"fmt"
	"strings"

	"github.com/oneshadab/hariken/pkg/database"
	"github.com/oneshadab/hariken/pkg/utils"
)

var availableCommands = map[string]QueryCommandHandler{
	"USE":    useCmd,
	"INSERT": insertCmd,
	"GET":    getCmd,
	"DELETE": deleteCmd,
	"UPDATE": updateCmd,
	"FILTER": filterCmd,
	"EXIT":   exitCmd,
}

type Query struct {
	commands []QueryCommand
}

type QueryContext struct {
	ses           *Session
	db            *database.Database
	tx            *database.Transaction
	err           *error
	processedCmds map[string]bool
}

type QueryCommand struct {
	name string
	args []string
}

type QueryCommandHandler func(ctx *QueryContext, args []string) (result string, err error)

func useCmd(ctx *QueryContext, args []string) (string, error) {
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

func insertCmd(ctx *QueryContext, args []string) (string, error) {
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

func getCmd(ctx *QueryContext, args []string) (string, error) {
	if len(args) > 1 {
		return fmt.Sprintf("Invalid syntax for `get`"), nil
	}

	tableName := args[0]
	ctx.tx.UseTable(tableName)
	ctx.tx.FetchAll()

	return ctx.resultTable()
}

func deleteCmd(ctx *QueryContext, args []string) (string, error) {
	for _, row := range ctx.tx.Result {
		ctx.tx.DeleteRow(row.Id())
	}

	return ctx.resultOK()
}

func updateCmd(ctx *QueryContext, args []string) (string, error) {
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

func filterCmd(ctx *QueryContext, args []string) (string, error) {
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

func exitCmd(ctx *QueryContext, args []string) (string, error) {
	return "KTHXBYE", nil
}

func (ctx *QueryContext) result() (string, error) {
	hasUsedShortResultCmd := false
	for _, shortResultCmd := range []string{"USE", "INSERT", "DELETE"} {
		hasUsedShortResultCmd = hasUsedShortResultCmd || ctx.processedCmds[shortResultCmd]
	}

	if hasUsedShortResultCmd {
		return ctx.resultOK()
	}
	return ctx.resultTable()
}

func (ctx *QueryContext) resultOK() (string, error) {
	return "OK", nil
}

func (ctx *QueryContext) resultTable() (string, error) {
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
