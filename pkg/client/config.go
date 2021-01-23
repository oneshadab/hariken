package client

import "fmt"

var config *Config

type Config struct {
	ConnString string
}

func LoadConfig(cfg *Config) error {
	err := cfg.Validate()
	if err != nil {
		return err
	}

	config = cfg
	return nil
}

func (c *Config) Validate() error {
	if c.ConnString == "" {
		return fmt.Errorf("ConnString not specified")
	}
	return nil
}
