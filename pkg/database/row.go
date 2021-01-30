package database

import (
	"encoding/json"
)

type Row struct {
	Column map[string]string
}

func NewRow() *Row {
	return &Row{
		Column: make(map[string]string),
	}
}

func (r *Row) Id() string {
	return r.Column["id"]
}

func (r *Row) setId(Id string) {
	r.Column["id"] = Id
}

// Todo: Replace `json` with custom serialize/deserialize
func (r *Row) Deserialize(data []byte) error {
	err := json.Unmarshal(data, r)
	if err != nil {
		return err
	}
	return nil
}

func (r *Row) Serialize() ([]byte, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	return data, nil
}
