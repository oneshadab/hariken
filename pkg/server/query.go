package server

import (
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
	err           error
	processedCmds map[string]bool
}

type QueryCommand struct {
	name string
	args []string
}

type QueryCommandHandler func(ctx *QueryContext, args []string)

type QueryError struct {
	msg string
}

func useCmd(ctx *QueryContext, args []string) {
	if ctx.Err() != nil {
		return
	}

	if len(args) > 1 || len(ctx.processedCmds) > 0 {
		ctx.err = NewQueryError("Invalid syntax for `use`")
		return
	}

	dbName := args[0]

	ctx.err = ctx.ses.useDatabase(dbName)
}

func insertCmd(ctx *QueryContext, args []string) {
	if ctx.Err() != nil {
		return
	}

	if len(args) <= 1 {
		ctx.err = NewQueryError("Invalid syntax for `insert`")
		return
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
}

func getCmd(ctx *QueryContext, args []string) {
	if ctx.Err() != nil {
		return
	}

	if len(args) > 1 {
		ctx.err = NewQueryError("Invalid syntax for `get`")
		return
	}

	tableName := args[0]
	ctx.tx.UseTable(tableName)
	ctx.tx.FetchAll()
}

func deleteCmd(ctx *QueryContext, args []string) {
	if ctx.Err() != nil {
		return
	}

	for _, row := range ctx.tx.Result {
		ctx.tx.DeleteRow(row.Id())
	}
}

func updateCmd(ctx *QueryContext, args []string) {
	if ctx.Err() != nil {
		return
	}

	if len(args) == 0 {
		ctx.err = NewQueryError("Invalid syntax for `update`")
		return
	}

	entries := make(map[string]string)
	for _, entry := range args {
		parts := strings.Split(entry, "=")
		key := parts[0]
		val := parts[1]
		entries[key] = val

		ctx.tx.UpdateAll(entries)
	}
}

func filterCmd(ctx *QueryContext, args []string) {
	if ctx.Err() != nil {
		return
	}

	if len(args) == 0 {
		ctx.err = NewQueryError("Invalid syntax for `filter`")
		return
	}

	for _, entry := range args {
		parts := strings.Split(entry, "=")
		key := parts[0]
		val := parts[1]

		ctx.tx.Filter(key, val)
	}
}

func exitCmd(ctx *QueryContext, args []string) {
	if ctx.Err() != nil {
		return
	}

}

func (ctx *QueryContext) result() (string, error) {
	if ctx.Err() != nil {
		return ctx.resultErr()
	}

	if ctx.processedCmds["exit"] {
		return ctx.resultExit()
	}

	if ctx.hasUsedCmds("INSERT", "DELETE", "USE") {
		return ctx.resultOK()
	}

	return ctx.resultTable()
}

func (ctx *QueryContext) resultOK() (string, error) {
	return "OK", nil
}

func (ctx *QueryContext) resultExit() (string, error) {
	return "KTHNXBYE", nil
}

func (ctx *QueryContext) resultErr() (string, error) {
	err := ctx.Err()

	if e, ok := err.(*QueryError); ok {
		return e.Error(), nil
	}

	return "", ctx.err
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

func (ctx *QueryContext) hasUsedCmds(cmdNames ...string) bool {
	for _, shortResultCmd := range cmdNames {
		return ctx.processedCmds[shortResultCmd]
	}
	return false
}

func (ctx *QueryContext) Err() error {
	if ctx.err != nil {
		return ctx.err
	}

	if ctx.tx.Err != nil {
		return ctx.tx.Err
	}

	return nil
}

func NewQueryError(msg string) *QueryError {
	return &QueryError{msg: msg}
}

func (e *QueryError) Error() string {
	return e.msg
}
