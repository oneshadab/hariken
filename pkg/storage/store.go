package storage

import "fmt"

type Store struct {
	data map[string]string
}

func (store *Store) get(key string) (string, error) {
	hasKey, err := store.has(key)

	if err != nil {
		return "", err
	}

	if !hasKey {
		return "", fmt.Errorf("key %s not found", key)
	}

	return store.data[key], nil
}

func (store *Store) set(key string, val string) error {
	store.data[key] = val
	return nil
}

func (store *Store) has(key string) (bool, error) {
	_, ok := store.data[key]
	return ok, nil
}

func (store *Store) delete(key string) error {
	hasKey, err := store.has(key)

	if err != nil {
		return err
	}

	if !hasKey {
		return fmt.Errorf("key %s not found", key)
	}

	delete(store.data, key)

	return nil
}
