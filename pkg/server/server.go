package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/oneshadab/hariken/pkg/storage"
)

type Server struct {
	listener net.Listener
	Store    storage.Store
}

func NewServer(connString string) (*Server, error) {
	var err error

	server := Server{}

	server.listener, err = net.Listen("tcp", connString)
	if err != nil {
		return nil, fmt.Errorf("Failed to create server: %s", err)
	}

	// Todo: read from disk instead of creating new store each time
	server.Store, err = storage.NewStore()
	if err != nil {
		return nil, err
	}

	return &server, nil
}

func (server *Server) WaitForConnections() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			fmt.Println("Failed to connect: ", err)
			continue
		}

		go server.handleConnection(conn)
	}
}

func (server *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	socketReader := bufio.NewReader(conn)
	socketWriter := bufio.NewWriter(conn)

	for {
		query, err := socketReader.ReadString('\n')
		if err != nil {
			fmt.Println("Something went wrong:", err)
			return
		}

		query = strings.TrimSuffix(query, "\n")
		parts := strings.Split(query, " ")

		cmd := parts[0]
		args := parts[1:]

		result, err := server.Exec(cmd, args)
		if err != nil {
			fmt.Println("Failed to execute query", err)
			return
		}

		_, err = socketWriter.WriteString(fmt.Sprintf("%s\n", result))
		if err != nil {
			fmt.Println("Failed to write to client", err)
			return
		}

		err = socketWriter.Flush()
		if err != nil {
			fmt.Println("Failed to flush buffer", err)
			return
		}
	}
}

func (S *Server) Exec(cmd string, args []string) (string, error) {
	CMD := strings.ToUpper(cmd)

	if CMD == "GET" {
		val, err := S.Store.Get(args[0])

		if err != nil {
			return "", err
		}

		if val == nil {
			return "nil", err
		}

		return fmt.Sprintf("\"%s\"", *val), nil
	}

	if CMD == "SET" {
		key := args[0]
		val := args[1]

		err := S.Store.Set(key, val)
		if err != nil {
			return "", err
		}

		return "OK", nil
	}

	if CMD == "HAS" {
		hasKey, err := S.Store.Has(args[0])

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
		err := S.Store.Delete(args[0])

		if err != nil {
			return "", err
		}

		return "OK", nil
	}

	if CMD == "EXIT" {
		return "KTHXBYE", nil
	}

	return "", fmt.Errorf("Command `%s` not found", cmd)
}
