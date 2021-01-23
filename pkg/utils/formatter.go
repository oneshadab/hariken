package utils

import (
	"bytes"

	"github.com/olekukonko/tablewriter"
)

func GenerateTable(headers []string, entries []map[string]string) string {
	buf := new(bytes.Buffer)

	table := tablewriter.NewWriter(buf)
	table.SetHeader(headers)

	for _, entry := range entries {
		vals := make([]string, 0, len(entry))
		for _, header := range headers {
			vals = append(vals, entry[header])
		}
		table.Append(vals)
	}
	table.Render()

	return buf.String()
}
