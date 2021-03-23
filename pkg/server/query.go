package server

import (
	"strings"
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

type QueryCommand struct {
	name string
	args []string
}

type QueryCommandHandler func(ctx *QueryContext, args []string)

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
