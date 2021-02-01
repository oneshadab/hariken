package storage

import (
	"bytes"
	"testing"
)

func TestPersistence(t *testing.T) {
	var err error

	storeDir := t.TempDir()

	// Part 1: store the value using a store
	store1, err := NewStore(storeDir)
	if err != nil {
		t.Fatal(err)
	}

	testData := struct {
		key   StoreKey
		value []byte
	}{
		key:   StoreKey{7},
		value: []byte("john"),
	}

	err = store1.Set(testData.key, testData.value)
	if err != nil {
		t.Fatal(err)
	}

	// Part 2: Try to read the value from another store
	store2, err := NewStore(storeDir)
	if err != nil {
		t.Fatal(err)
	}

	value, err := store2.Get(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	// 2nd store should read the same value stored by store
	if !bytes.Equal(testData.value, value) {
		t.Fatalf("Expected %v got %v", testData.value, value)
	}

	// Part 3: We delete the key and try to read it again
	err = store2.Delete(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	// Open another store at the same location
	store3, err := NewStore(storeDir)
	if err != nil {
		t.Fatal(err)
	}

	hasValue, err := store3.Has(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	// Value should be deleted
	if hasValue != false {
		t.Fatalf("Expected %v got %v", false, hasValue)
	}
}

func TestFlush(t *testing.T) {
	testData := struct {
		key   StoreKey
		value []byte
	}{
		key:   StoreKey{7},
		value: []byte("john"),
	}

	storeDir := t.TempDir()

	store, err := NewStore(storeDir)
	if err != nil {
		t.Fatal(err)
	}

	// Part 1: Test if flushed value can be accessed
	err = store.Set(testData.key, testData.value)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Flush()
	if err != nil {
		t.Fatal(err)
	}

	keys, err := store.memTable.Keys()
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) > 0 {
		t.Fatalf("Commit log not flushed")
	}

	value, err := store.Get(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(testData.value, value) {
		t.Fatalf("Expected %v got %v", testData.value, value)
	}

	// Part 2: Test if flushed deleted value is properly removed
	err = store.Delete(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Flush()
	if err != nil {
		t.Fatal(err)
	}

	hasValue, err := store.Has(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	if hasValue != false {
		t.Fatalf("Expected %v got %v", false, hasValue)
	}

	// Part 3: Test if non-flushed values are accessed properly
	err = store.Set(testData.key, testData.value)
	if err != nil {
		t.Fatal(err)
	}

	value, err = store.Get(testData.key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(testData.value, value) {
		t.Fatalf("Expected %v got %v", testData.value, value)
	}
}
