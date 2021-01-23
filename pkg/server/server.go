package server

import (
	"bufio"
	"fmt"
	"net"
)

type Server struct {
	listener net.Listener
}

func NewServer(cfg *Config) (*Server, error) {
	err := LoadConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("Failed to create server: %s", err)
	}

	server := Server{}

	server.listener, err = net.Listen("tcp", config.ConnString)
	if err != nil {
		return nil, fmt.Errorf("Failed to create server: %s", err)
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

		// Start new session in new thread
		go func() {
			defer conn.Close()

			connReader := bufio.NewReader(conn)
			connWriter := bufio.NewWriter(conn)

			session, err := NewSession(connReader, connWriter)
			if err != nil {
				msg := fmt.Sprintf("Failed to initialize session: %v", err)
				fmt.Println(msg)

				_, err = connWriter.WriteString(msg) // Send message to client as well
				if err != nil {
					fmt.Sprintln("Failed to send message to client")
				}

				return
			}

			err = session.Start()
			if err != nil {
				fmt.Printf("Session exited due to error: %v", err)
			}
		}()
	}
}
