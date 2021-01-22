package storage

import (
	"io/ioutil"
	"testing"
)

func TestPersistence(t *testing.T) {
	var err error

	testDir := t.TempDir()

	testFile, err := ioutil.TempFile(testDir, "")
	if err != nil {
		t.Fatal(err)
	}
	defer testFile.Close()

	databaseFilePath := testFile.Name()

	// Part 1: Database the value using a database
	database1, err := LoadDatabase(databaseFilePath)
	if err != nil {
		t.Fatal(err)
	}

	testData := struct {
		key   string
		value string
	}{
		key:   "name",
		value: "john",
	}

	err = database1.Set(testData.key, testData.value)
	if err != nil {
		t.Fatal(err)
	}

	// Part 2: Try to read the value from another database
	database2, err := LoadDatabase(databaseFilePath)
	if err != nil {
		t.Fatal(err)
	}

	value, err := database2.Get(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	// 2nd database should read the same value databased by database
	if testData.value != *value {
		t.Fatalf("Exptected %v got %v", testData.value, value)
	}

	// Part 3: We delete the key and try to read it again
	database2.Delete(testData.key)

	// Open another database at the same location
	database3, err := LoadDatabase(databaseFilePath)
	if err != nil {
		t.Fatal(err)
	}

	hasValue, err := database3.Has(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	// Value should be deleted
	if hasValue != false {
		t.Fatalf("Exptected %v got %v", false, hasValue)
	}
}
