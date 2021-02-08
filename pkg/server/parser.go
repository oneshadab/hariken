package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/oneshadab/hariken/pkg/utils"
)

func tokenize(cmd string) ([]string, error) {
	tokens := make([]string, 0)
	parts := strings.Split(cmd, "|")

	for _, s := range parts {
		s = strings.TrimSpace(s)
		tokens = append(tokens, s)
	}

	return tokens, nil
}

func ExecCommand(S *Session, query string) (string, error) {
	tx := S.db.NewTransaction()
	tokens, err := tokenize(query)

	if err != nil {
		return "", err
	}

	// Todo: Make commands in a chain atomic

	for _, token := range tokens {
		parts := strings.Split(token, " ")
		cmd := strings.ToUpper(parts[0])
		args := parts[1:]

		switch cmd {

		case "USE":
			if len(args) > 1 || len(tx.ProcessedCommandTypes) > 0 {
				return "", errors.New("invalid command")
			}

			dbName := args[0]
			err := S.useDatabase(dbName)
			if err != nil {
				return "", err
			}

		case "INSERT":
			if len(args) <= 1 {
				return "", errors.New("invalid command")
			}

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
			tx.InsertRow(entries)

		case "GET":
			if len(args) > 1 {
				return "", errors.New("invalid command")
			}

			tableName := args[0]
			tx.UseTable(tableName)
			tx.FetchAll()

		case "DELETE":
			for _, row := range tx.Result {
				tx.DeleteRow(row.Id())
			}

		case "FILTER":
			if len(args) == 0 {
				return "", errors.New("invalid command")
			}

			for _, entry := range args {
				parts := strings.Split(entry, "=")
				key := parts[0]
				val := parts[1]

				tx.Filter(key, val)
			}

		case "EXIT":
			return "KTHXBYE", nil

		default:
			return fmt.Sprintf("Command `%s` not found", cmd), nil
		}

		if tx.Err != nil {
			return "", tx.Err
		}

		tx.ProcessedCommandTypes[cmd] = true
	}

	if tx.ProcessedCommandTypes["USE"] || tx.ProcessedCommandTypes["INSERT"] == true || tx.ProcessedCommandTypes["DELETE"] {
		return "OK", nil
	}

	headers, err := tx.Table.Columns()
	if err != nil {
		return "", err
	}

	result := []map[string]string{}
	for _, v := range tx.Result {
		result = append(result, v.Column)
	}

	output := utils.GenerateTable(headers, result)
	return output, nil
}
