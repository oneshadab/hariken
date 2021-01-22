package server

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/oneshadab/hariken/pkg/database"
)

type Session struct {
	config *Config

	db     *database.Database
	reader bufio.Reader
	writer bufio.Writer
}

func NewSession(connReader *bufio.Reader, connWriter *bufio.Writer, config *Config) (*Session, error) {
	session := Session{
		config: config,
		reader: *connReader,
		writer: *connWriter,
	}

	err := session.useDatabase(config.DefaultDatabaseName)
	if err != nil {
		return nil, err
	}

	return &session, nil
}

func (S *Session) Start() error {
	for {
		query, err := S.reader.ReadString('\n')
		if err != nil {
			return err
		}

		result, err := S.Exec(query)
		if err != nil {
			return err
		}

		_, err = S.writer.WriteString(fmt.Sprintf("%s\n", result))
		if err != nil {
			return err
		}

		err = S.writer.Flush()
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

		rows, err := S.db.Query(tableName).Get(database.RowId(rowId)).Exec()
		if err != nil {
			return "", err
		}
		if len(rows) == 0 {
			return "nil", nil
		}

		val, err := rows[0].Serialize()
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("\"%s\"", val), nil

	case "UPSERT":
		tableName := args[0]
		rowData := args[1]

		row := &database.Row{}
		row.Deserialize(&rowData)

		_, err := S.db.Query(tableName).Upsert(row).Exec()
		if err != nil {
			return "", err
		}

		val, err := row.Serialize()
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("\"%s\"", val), nil

	case "DELETE":
		tableName := args[0]
		rowId := args[1]

		id := database.RowId(rowId)
		row := &database.Row{Id: &id}

		_, err := S.db.Query(tableName).Delete(row).Exec()

		if err != nil {
			return "", err
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
