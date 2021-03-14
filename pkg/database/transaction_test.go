package database

import (
	"sync"
	"testing"
	"time"
)

func TestIsolation(t *testing.T) {
	testData := []map[string]string{
		{"name": "john"},
		{"name": "alex"},
	}

	// Setup reader and writer thread
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

	// Part 1: Write data in writer thread and read data from reader thread
	writer.exec(func() {
		writeTx.InsertRow(testData[0])
	})

	writer.exec(func() {
		writeTx.Commit()
	})

	writer.join()

	rowId := writeTx.Result[0].Id()
	reader.exec(func() {
		readTx.FetchRow(rowId)
	})

	reader.join()

	if testData[0]["name"] != readTx.Result[0].Column["name"] {
		t.Fatalf("Expected %s got %s\n", testData[0]["name"], readTx.Result[0].Column["name"])
	}

	// Part 2: Write data in writer thread but try to read it before it's commited

	writer.exec(func() {
		writeTx.UpdateAll(testData[1])
	})
	writer.join()

	reader.exec(func() {
		readTx.FetchRow(rowId)
	})

	// Give the reader some time to try to read the data
	time.Sleep(50 * time.Millisecond)

	writer.exec(func() {
		writeTx.Commit()
	})

	writer.join()
	reader.join()

	// Reader should see the updated data, even though read was issued before the updated data was commited
	if testData[1]["name"] != readTx.Result[0].Column["name"] {
		t.Fatalf("Expected %s got %s\n", testData[1]["name"], readTx.Result[0].Column["name"])
	}
}

type Worker struct {
	commands chan func()
	wg       sync.WaitGroup
}

// Creates a worker and starts it in the background
func startWorker() *Worker {
	worker := &Worker{
		commands: make(chan func()),
	}

	go func() {
		for cmd := range worker.commands {
			cmd()
			worker.wg.Done()
		}
	}()

	return worker
}

func (w *Worker) exec(cmd func()) {
	w.wg.Add(1)
	w.commands <- cmd
}

func (w *Worker) join() {
	w.wg.Wait()
}
