package srctype

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"os"
	"strings"
)

type csvType struct {
	fileName string
	file     *os.File
	reader   *csv.Reader
	colNames []string
	records  [][]string
}

// NewCsvStrConn create connector for csv string
func NewCsvStrConn(csvStr string) (Connector, error) {
	//create Reader
	r := csv.NewReader(strings.NewReader(csvStr))
	r.FieldsPerRecord = -1
	colNames, err := r.Read()
	if err != nil {
		return nil, err
	}
	return &csvType{reader: r, colNames: colNames}, nil
}

// NewCsvFileConn create connector for csv file
func NewCsvFileConn(fileName string) (Connector, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	//creat Reader
	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1
	colNames, err := r.Read()
	if err != nil {
		return nil, err
	}
	return &csvType{fileName: fileName, reader: r, colNames: colNames}, nil
}

func (c *csvType) Read() ([]string, error) {
	return c.reader.Read()
}

func (c *csvType) ColNames() ([]string, error) {
	return c.colNames, nil
}

func (c *csvType) ReadAll() ([][]string, error) {
	return c.reader.ReadAll()
}

func (c *csvType) ReadStr() (string, error) {
	records, err := c.ReadAll()
	if err != nil {
		return "", err
	}
	output := ""
	buf := bytes.NewBufferString(output)
	w := csv.NewWriter(buf)
	w.WriteAll(records)
	if err := w.Error(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (c *csvType) Close() {
	if c.file != nil {
		c.file.Close()
	}
}

func (c *csvType) ReadRowToChan(chan interface{}) {
	panic("not implemented")
	return
}
