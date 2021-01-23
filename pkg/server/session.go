package server

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/oneshadab/hariken/pkg/database"
	"github.com/oneshadab/hariken/pkg/protocol"
	"github.com/oneshadab/hariken/pkg/utils"
)

type Session struct {
	config *Config

	db     *database.Database
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewSession(connReader *bufio.Reader, connWriter *bufio.Writer, config *Config) (*Session, error) {
	session := Session{
		config: config,
		reader: connReader,
		writer: connWriter,
	}

	err := session.useDatabase(config.DefaultDatabaseName)
	if err != nil {
		return nil, err
	}

	return &session, nil
}

func (S *Session) Start() error {
	for {
		query, err := protocol.ReadMessage(S.reader)
		if err != nil {
			return err
		}

		result, err := S.Exec(query)
		if err != nil {
			return err
		}

		err = protocol.WriteMessage(S.writer, result)
		if err != nil {
			return err
		}
	}
}

func (S *Session) Exec(query string) (string, error) {
	query = strings.TrimSuffix(query, "\n")
	parts := strings.Split(query, " ")

	cmd := parts[0]
	args := parts[1:]

	cmd = strings.ToUpper(cmd)
	switch cmd {
	case "USE":
		dbName := args[0]

		err := S.useDatabase(dbName)
		if err != nil {
			return "", err
		}
		return "OK", nil

	case "GET":
		tableName := args[0]
		rowId := args[1]

		tx := S.db.NewTransaction()
		tx.UseTable(tableName)
		tx.FetchRow(rowId)
		if tx.Err != nil {
			return "", tx.Err
		}

		headers, err := tx.Table.Columns()
		if err != nil {
			return "", err
		}

		row := tx.Result[0]
		if row == nil {
			return "nil", nil
		}

		output := utils.GenerateTable(headers, []map[string]string{row.Column})
		return output, nil

	case "UPSERT":
		tableName := args[0]

		entries := make(map[string]string)
		for _, entry := range args[1:] {
			parts := strings.Split(entry, "=")
			key := parts[0]
			val := parts[1]

			entries[key] = val
		}

		tx := S.db.NewTransaction()
		tx.UseTable(tableName)
		tx.UpsertRow(entries)

		if tx.Err != nil {
			return "", tx.Err
		}

		headers, err := tx.Table.Columns()
		if err != nil {
			return "", err
		}
		row := tx.Result[0]

		output := utils.GenerateTable(headers, []map[string]string{row.Column})
		return output, nil

	case "DELETE":
		tableName := args[0]
		rowId := args[1]

		tx := S.db.NewTransaction()
		tx.UseTable(tableName)
		tx.DeleteRow(rowId)

		if tx.Err != nil {
			return "", tx.Err
		}

		return "OK", nil

	case "EXIT":
		return "KTHXBYE", nil

	default:
		return fmt.Sprintf("Command `%s` not found", cmd), nil
	}
}

func (S *Session) useDatabase(dbName string) error {
	db, err := database.LoadDatabase(S.config.DatabasePath(dbName))
	if err != nil {
		return err
	}

	S.db = db

	return nil
}
