package server

import (
	"bufio"
	"fmt"

	"github.com/oneshadab/hariken/pkg/database"
	"github.com/oneshadab/hariken/pkg/protocol"
)

type Session struct {
	db *database.Database
}

func NewSession() (*Session, error) {
	session := Session{}

	err := session.useDatabase(config.DefaultDatabaseName)
	if err != nil {
		return nil, err
	}

	return &session, nil
}

func (S *Session) Start(reader *bufio.Reader, writer *bufio.Writer) error {
	//Todo: handle terminating session
	for {
		query, err := protocol.ReadMessage(reader)
		if err != nil {
			return err
		}

		result, err := S.Exec(query)
		if err != nil {
			return err
		}

		err = protocol.WriteMessage(writer, result)
		if err != nil {
			return err
		}
	}
}

func (S *Session) Exec(queryStr string) (string, error) {
	ctx := &QueryContext{
		tx:            S.db.NewTransaction(),
		processedCmds: make(map[string]bool),
	}

	defer ctx.tx.Cleanup()

	q, err := parseQuery(queryStr)
	if err != nil {
		return "", err
	}

	// Todo: Make commands in a chain atomic
	for _, cmd := range q.commands {
		fn, ok := availableCommands[cmd.name]
		if !ok {
			ctx.err = NewQueryError(fmt.Sprintf("Command `%s` not found", cmd.name))
			break
		}

		fn(ctx, cmd.args)
		ctx.processedCmds[cmd.name] = true
	}

	return ctx.result()
}

func (S *Session) useDatabase(dbName string) error {
	db, err := database.LoadDatabase(config.DatabasePath(dbName))
	if err != nil {
		return err
	}

	S.db = db

	return nil
}
