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

	// Create a store at storeFilePath
	store, err := NewStore(storeFilePath)
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

	err = store.Set(testData.key, testData.value)
	if err != nil {
		t.Fatal(err)
	}

	// Open 2nd store with the same filepath
	otherStore, err := NewStore(storeFilePath)
	if err != nil {
		t.Fatal(err)
	}

	value, err := otherStore.Get(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	// otherStore should read the same value stored by store
	if testData.value != *value {
		t.Fatalf("Exptected %v got %v", testData.value, value)
	}
}
