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
func (r *Row) Deserialize(data *string) error {
	err := json.Unmarshal([]byte(*data), r)
	if err != nil {
		return err
	}
	return nil
}

func (r *Row) Serialize() (*string, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	s := string(data)
	return &s, nil
}
