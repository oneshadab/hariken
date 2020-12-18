package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/oneshadab/hariken/pkg/session"
)

func main() {
	session := session.NewSession()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("$ ")

		line, err := reader.ReadString('\n')
		if err != nil {
			panic("Failed to read line from console")
		}

		line = strings.TrimSuffix(line, "\n")
		words := strings.Split(line, " ")

		cmd := words[0]
		args := words[1:]

		output, err := session.Exec(cmd, args)

		if err != nil {
			fmt.Println("ERROR:", err)
			continue
		}

		fmt.Println(output)
	}
}
