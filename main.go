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

	fmt.Println("Hariken shell version v0.1")

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

		if strings.ToUpper(cmd) == "EXIT" {
			fmt.Println("KTHXBYE")
			break
		}

		output, err := session.Exec(cmd, args)

		if err != nil {
			fmt.Println("ERROR:", err)
			continue
		}

		fmt.Println(output)
	}
}
