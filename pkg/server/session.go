package server

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/oneshadab/hariken/pkg/database"
)

type Session struct {
	config *Config

	db     database.Database
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

		if val == nil {
			return "nil", err
		}

		return fmt.Sprintf("\"%s\"", *val), nil

	case "SET":
		key := args[0]
		val := args[1]

		err := S.db.Set(key, val)
		if err != nil {
			return "", err
		}

		return "OK", nil

	case "HAS":
		hasKey, err := S.db.Has(args[0])

		if err != nil {
			return "", err
		}

		if hasKey {
			return "True", nil
		} else {
			return "False", nil
		}

	case "DELETE":
		err := S.db.Delete(args[0])

		if err != nil {
			return "", err
		}

		return "OK", nil

	case "USE":
		err := S.useDatabase(args[0])
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
	db, err := storage.LoadDatabase(S.config.DatabasePath(dbName))
	if err != nil {
		return err
	}

	S.db = db

	return nil
}
