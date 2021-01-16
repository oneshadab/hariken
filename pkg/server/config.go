package server

type Config struct {
	ConnString  string
	StorageRoot string
}

func (c *Config) DefaultStorePath() *string {
	storePath := "temp/temp.db"
	return &storePath
}
