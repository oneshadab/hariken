package server

import "path/filepath"

type Config struct {
	ConnString  string
	StorageRoot string

	DefaultStoreName *string // The default store to use when a session is started
}

func (c *Config) StorePath(storeName string) string {
	return filepath.Join(c.StorageRoot, storeName)
}
