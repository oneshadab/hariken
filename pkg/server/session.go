package server

import (
	"bufio"
	"strings"

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

func (S *Session) Exec(query string) (string, error) {
	query = strings.TrimSuffix(query, "\n")

	commandHandlers := map[string]interface{}{
		"startTransaction": S.db.NewTransaction,
		"useDatabase":      S.useDatabase,
	}

	return ExecCommand(query, commandHandlers)
}

func (S *Session) useDatabase(dbName string) error {
	db, err := database.LoadDatabase(config.DatabasePath(dbName))
	if err != nil {
		return err
	}

	S.db = db

	return nil
}
