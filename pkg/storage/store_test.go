package storage

import (
	"io/ioutil"
	"log"
	"testing"
)

func TestPersistence(t *testing.T) {
	var err error

	testDir := t.TempDir()

	testFile, err := ioutil.TempFile(testDir, "")
	if err != nil {
		t.Fatal(err)
	}

	defer func(){
		err = testFile.Close()
		if err != nil{
			log.Fatal(err)
		}
	}()

	storeFilePath := testFile.Name()

	// Part 1: store the value using a store
	store1, err := NewStore(storeFilePath)
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

	err = store1.Set(testData.key, testData.value)
	if err != nil {
		t.Fatal(err)
	}

	// Part 2: Try to read the value from another store
	store2, err := NewStore(storeFilePath)
	if err != nil {
		t.Fatal(err)
	}

	value, err := store2.Get(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	// 2nd store should read the same value stored by store
	if testData.value != *value {
		t.Fatalf("Expected %v got %v", testData.value, value)
	}

	// Part 3: We delete the key and try to read it again
	err = store2.Delete(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	// Open another store at the same location
	store3, err := NewStore(storeFilePath)
	if err != nil {
		t.Fatal(err)
	}

	hasValue, err := store3.Has(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	// Value should be deleted
	if hasValue != false {
		t.Fatalf("Exptected %v got %v", false, hasValue)
	}
}
