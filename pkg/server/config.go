package server

import (
	"fmt"
	"path/filepath"
)

var config *Config

type Config struct {
	ConnString  string
	StorageRoot string

	DefaultDatabaseName string // The default database to use when a session is started
}

func LoadConfig(cfg *Config) error {
	err := cfg.Validate()
	if err != nil {
		return err
	}

	config = cfg
	return nil
}

func (c *Config) DatabasePath(databaseName string) string {
	return filepath.Join(c.StorageRoot, "database", databaseName)
}

func (c *Config) Validate() error {
	if c.ConnString == "" {
		return fmt.Errorf("ConnString not specified")
	}
	if c.StorageRoot == "" {
		return fmt.Errorf("StorageRoot not specified")
	}
	if c.DefaultDatabaseName == "" {
		return fmt.Errorf("DefaultDatabaseName not specified")
	}
	return nil
}
