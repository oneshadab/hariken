package server

import (
	"bufio"

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
	query, err := parseQuery(queryStr)
	if err != nil {
		return "", err
	}

	// We create a context with all the relevant transaction/db/session info
	ctx := S.newQueryContext()
	defer ctx.Cleanup()

	// Each command modifies the context
	for _, cmd := range query.commands {
		ctx.exec(cmd)
	}

	// Finally we return the result/errror stored in the context
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

func (S *Session) newQueryContext() *QueryContext {
	return &QueryContext{
		ses:           S,
		db:            S.db,
		tx:            S.db.NewTransaction(),
		processedCmds: make(map[string]bool),
	}
}
