package client

import "fmt"

type Config struct {
	ConnString string
}

func (c *Config) Validate() error {
	if c.ConnString == "" {
		return fmt.Errorf("ConnString not specified")
	}
	return nil
}
