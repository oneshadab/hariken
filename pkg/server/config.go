package server

import (
	"fmt"
	"path/filepath"
)

type Config struct {
	ConnString  string
	StorageRoot string

	DefaultStoreName string // The default store to use when a session is started
}

func (c *Config) StorePath(storeName string) string {
	return filepath.Join(c.StorageRoot, "store", storeName)
}

func (c *Config) Validate() error {
	if c.ConnString == "" {
		return fmt.Errorf("ConnString not specified")
	}
	if c.StorageRoot == "" {
		return fmt.Errorf("StorageRoot not specified")
	}
	if c.DefaultStoreName == "" {
		return fmt.Errorf("DefaultStoreName not specified")
	}
	return nil
}
