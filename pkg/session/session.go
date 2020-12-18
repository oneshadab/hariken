package session

import (
	"fmt"
	"strings"

	"github.com/oneshadab/hariken/pkg/storage"
)

type Session struct {
	store *storage.Store
}

func NewSession() *Session {
	return &Session{
		store: storage.NewStore(),
	}
}

func (S *Session) Exec(cmd string, args []string) (string, error) {
	CMD := strings.ToUpper(cmd)

	if CMD == "GET" {
		val, err := S.store.Get(args[0])

		if err != nil {
			return "", err
		}

		return val, nil
	}

	if CMD == "SET" {
		err := S.store.Set(args[0], args[1])

		if err != nil {
			return "", err
		}

		return args[1], nil
	}

	if CMD == "HAS" {
		hasKey, err := S.store.Has(args[0])

		if err != nil {
			return "", err
		}

		if hasKey {
			return "True", nil
		} else {
			return "False", nil
		}
	}

	if CMD == "DELETE" {
		err := S.store.Delete(args[0])

		if err != nil {
			return "", err
		}

		return "OK", nil
	}

	return "", fmt.Errorf("Command `%s` not found", CMD)
}
