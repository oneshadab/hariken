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

	storeFilePath := testFile.Name()

	// Part 1: Store the value using a store
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
		t.Fatalf("Exptected %v got %v", testData.value, value)
	}

	// Part 3: We delete the key and try to read it again
	store2.Delete(testData.key)

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
