package storage

import "fmt"

type Store struct {
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (store *Store) Get(key string) (string, error) {
	hasKey, err := store.Has(key)

	if err != nil {
		return "", err
	}

	if !hasKey {
		return "", fmt.Errorf("key %s not found", key)
	}

	return store.data[key], nil
}

func (store *Store) Set(key string, val string) error {
	store.data[key] = val
	return nil
}

func (store *Store) Has(key string) (bool, error) {
	_, ok := store.data[key]
	return ok, nil
}

func (store *Store) Delete(key string) error {
	hasKey, err := store.Has(key)

	if err != nil {
		return err
	}

	if !hasKey {
		return fmt.Errorf("key %s not found", key)
	}

	delete(store.data, key)

	return nil
}
