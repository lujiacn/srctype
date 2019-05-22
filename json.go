package srctype

import (
	"encoding/json"
	"fmt"
)

type jsonType struct {
	colNames []string
	records  []map[string]interface{}
}

func NewJsonConn(apiUrl, user, passwd string, proxyUrl string) (Connector, error) {
	bodyBytes, err := httpConn(apiUrl, user, passwd, proxyUrl)
	if err != nil {
		return nil, err
	}

	output := []map[string]interface{}{}
	err = json.Unmarshal(bodyBytes, &output)
	if err != nil {
		return nil, err
	}

	jt := new(jsonType)
	// get colNames
	if len(output) > 0 {
		colNames := []string{}
		for k, _ := range output[0] {
			colNames = append(colNames, k)
		}
		jt.colNames = colNames
	}

	jt.records = output
	return jt, nil
}

func (m *jsonType) Close() {
	return
}

func (m *jsonType) ColNames() ([]string, error) {
	return m.colNames, nil
}
func (m *jsonType) ReadAll() ([][]string, error) {
	output := [][]string{}

	for _, row := range m.records {
		line := []string{}
		for _, col := range m.colNames {
			switch row[col].(type) {
			case string:
				line = append(line, row[col].(string))
			case nil:
				line = append(line, "")
			case map[string]interface{}:
				jsStr, err := json.Marshal(row[col].(map[string]interface{}))
				if err != nil {
					line = append(line, fmt.Sprintf("%v", row[col]))
				} else {
					line = append(line, string(jsStr))
				}
			default:
				line = append(line, fmt.Sprintf("%v", row[col]))
			}
		}
		output = append(output, line)
	}

	return output, nil
}

func (m *jsonType) Read() ([]string, error) {
	return nil, fmt.Errorf("Cannot use read for json format")
}

func (m *jsonType) ReadStr() (string, error) {
	return "", fmt.Errorf("Cannot use readStr for json format")
}
