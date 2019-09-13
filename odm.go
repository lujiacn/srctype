package srctype

import (
	"fmt"

	"github.com/lujiacn/rws"
)

type odmType struct {
	colNames []string
	records  [][]string
}

func NewODMConn(apiUrl, user, passwd string, proxyUrl string) (Connector, error) {
	// read remote
	body, err := rws.RwsRead(apiUrl, user, passwd, proxyUrl)
	if err != nil {
		return nil, err
	}
	//results, err := rws.RwsToMap(body)

	rowMap, colNames, err := rws.RwsToFlatMap(body)

	if err != nil {
		return nil, err
	}
	out := &odmType{colNames: colNames}

	// tmap to records [][]string
	records := [][]string{}

	for _, row := range rowMap {
		item := []string{}
		for _, col := range colNames {
			item = append(item, row[col])
		}
		records = append(records, item)
	}
	out.records = records
	return out, nil
}

func (m *odmType) Read() ([]string, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *odmType) ReadStr() (string, error) {
	return "", fmt.Errorf("Not implemented")
}

func (m *odmType) Close() {
	return
}

func (m *odmType) ReadAll() ([][]string, error) {
	return m.records, nil
}

func (m *odmType) ColNames() ([]string, error) {
	return m.colNames, nil

}

func (c *odmType) ReadRowToChan(chan interface{}) {
	panic("not implemented")
	return
}
