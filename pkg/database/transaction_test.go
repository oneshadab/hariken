package database

import (
	"testing"
	"time"
)

func TestIsolation(t *testing.T) {
	testData := []map[string]string{
		{"name": "john"},
		{"name": "alex"},
	}

	writer := startWorker()
	reader := startWorker()

	db, err := LoadDatabase(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	writeTx := db.NewTransaction()
	readTx := db.NewTransaction()

	writeTx.UseTable("test")
	readTx.UseTable("test")

	writer.exec(func() {
		writeTx.InsertRow(testData[0])
	})
	writer.exec(func() {
		writeTx.Commit()
	})

	time.Sleep(50 * time.Millisecond)

	rowId := writeTx.Result[0].Id()
	reader.exec(func() {
		readTx.FetchRow(rowId)
	})

	time.Sleep(50 * time.Millisecond)

	if testData[0]["name"] != readTx.Result[0].Column["name"] {
		t.Fatalf("Expected %s got %s\n", testData[0]["name"], readTx.Result[0].Column["name"])
	}

	// Part 2

	writer.exec(func() {
		writeTx.UpdateAll(testData[1])
	})
	time.Sleep(50 * time.Millisecond)

	reader.exec(func() {
		readTx.FetchRow(rowId)
	})
	time.Sleep(50 * time.Millisecond)

	writer.exec(func() {
		writeTx.Commit()
	})
	time.Sleep(50 * time.Millisecond)

	if testData[1]["name"] != readTx.Result[0].Column["name"] {
		t.Fatalf("Expected %s got %s\n", testData[1]["name"], readTx.Result[0].Column["name"])
	}
}

type Worker struct {
	commands chan func()
}

// Creates a worker and starts it in the background
func startWorker() *Worker {
	worker := &Worker{
		commands: make(chan func()),
	}

	go func() {
		for cmd := range worker.commands {
			cmd()
		}
	}()

	return worker
}

func (w *Worker) exec(cmd func()) {
	w.commands <- cmd
}
