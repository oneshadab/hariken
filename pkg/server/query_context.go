package server

import (
	"github.com/oneshadab/hariken/pkg/database"
	"github.com/oneshadab/hariken/pkg/utils"
)

type QueryContext struct {
	ses           *Session
	db            *database.Database
	tx            *database.Transaction
	err           error
	processedCmds map[string]bool
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
