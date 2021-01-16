package server

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/oneshadab/hariken/pkg/storage"
)

type Session struct {
	Store  storage.Store
	reader bufio.Reader
	writer bufio.Writer
}

func NewSession(connReader *bufio.Reader, connWriter *bufio.Writer, config *Config) (*Session, error) {
	defaultStorePath := *config.DefaultStorePath()
	store, err := storage.NewStore(defaultStorePath)
	if err != nil {
		return nil, err
	}

	session := Session{
		Store:  store,
		reader: *connReader,
		writer: *connWriter,
	}

	return &session, nil
}

func (S *Session) Start() error {
	for {
		query, err := S.reader.ReadString('\n')
		if err != nil {
			return err
		}

		query = strings.TrimSuffix(query, "\n")
		parts := strings.Split(query, " ")

		cmd := parts[0]
		args := parts[1:]

		result, err := S.Exec(cmd, args)
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

func (S *Session) Exec(cmd string, args []string) (string, error) {
	CMD := strings.ToUpper(cmd)

	switch CMD {
	case "GET":
		val, err := S.Store.Get(args[0])

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

		err := S.Store.Set(key, val)
		if err != nil {
			return "", err
		}

		return "OK", nil

	case "HAS":
		hasKey, err := S.Store.Has(args[0])

		if err != nil {
			return "", err
		}

		if hasKey {
			return "True", nil
		} else {
			return "False", nil
		}

	case "DELETE":
		err := S.Store.Delete(args[0])

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
