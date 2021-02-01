package server

import (
	"strings"
)

type Token struct {
	token string
}

func Tokenize(cmd string) ([]Token, error) {
	tokens := make([]Token, 0)
	parts := strings.Split(cmd, "|")

	for _, s := range parts {
		s = strings.TrimSpace(s)
		tokens = append(tokens, Token{s})
	}

	return tokens, nil
}
