package database

import (
	"encoding/json"
)

type RowId string

type Row struct {
	Id     *RowId
	Column map[string]string
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
