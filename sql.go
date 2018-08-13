package srctype

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	ora "gopkg.in/rana/ora.v4"
)

type sqlType struct {
	db        *sql.DB
	rows      *sql.Rows
	sqlScript string
}

// dbConnect create *sql.DB connection
// srcType: oracle, mysql
// sqlConnStr:
// mysql: tcp(ip_address:3306)/db_name
// oracle: ip_address:1521/XE
func dbConnect(srcType, sqlConnStr string) (*sql.DB, error) {
	var err error
	var db *sql.DB
	switch srcType {
	case "mysql":
		db, err = sql.Open("mysql", sqlConnStr)
	case "oracle":
		db, err = sql.Open("ora", sqlConnStr)
	}

	if err != nil {
		return nil, err
	}
	return db, nil
}

//init is db initiation
func (o *sqlType) init() error {
	sqlScript := o.sqlScript
	rows, err := o.db.Query(sqlScript)
	if err != nil {
		return err
	}
	o.rows = rows
	return nil
}

func NewSqlConn(srcType, sqlConnStr, sqlScript string) (Connector, error) {
	var err error
	o := new(sqlType)
	o.db, err = dbConnect(srcType, sqlConnStr)
	if err != nil {
		return nil, err
	}
	o.sqlScript = sqlScript
	if err = o.init(); err != nil {
		return nil, err
	}

	return o, nil
}

//ReadAll read all result and close the row at end. Can not be used with Read() together
func (o *sqlType) Read() ([]string, error) {
	rows := o.rows
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	count := len(columns)
	readCols := make([]interface{}, count)
	rawCols := make([]interface{}, count)

	if rows.Next() {
		for i := range columns {
			readCols[i] = &rawCols[i]
		}
		err = rows.Scan(readCols...)
		if err != nil {
			return nil, err
		}
		record := assertTypeArray(columns, rawCols)
		return record, nil
	}
	return nil, io.EOF
}

//ColNames get record column names
func (o *sqlType) ColNames() (colNames []string, err error) {
	colNames, err = o.rows.Columns()
	if err != nil {
		return nil, err

	}
	return colNameReplace(colNames), nil
}

//
func (o *sqlType) ReadAll() (output [][]string, err error) {
	defer o.rows.Close()
	if o.rows == nil {
		return nil, errors.New("No connection.")
	}

	for {
		row, err := o.Read()
		if err != nil && err != io.EOF {
			return nil, err
		}
		output = append(output, row)
	}
	return
}

func (o *sqlType) ReadStr() (string, error) {
	records, err := o.ReadAll()
	if err != nil {
		return "", err
	}
	output := ""
	buf := bytes.NewBufferString(output)
	w := csv.NewWriter(buf)
	w.WriteAll(records)
	w.Flush()
	if err := w.Error(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

//Close function
func (o *sqlType) Close() {
	if o.rows != nil {
		o.rows.Close()
	}
	if o.db != nil {
		o.db.Close()
	}
}

//ColNameReplace replace . with _  due to mongo restriction
func colNameReplace(colNames []string) []string {
	newColNames := []string{}
	for _, colName := range colNames {
		newColName := strings.Replace(colName, ".", "_", -1)
		newColNames = append(newColNames, newColName)
	}
	return newColNames
}

func assertTypeArray(cols []string, rawCols []interface{}) []string {
	resultCols := make([]string, len(cols))
	for i, _ := range cols {
		val := rawCols[i]
		if val == nil {
			resultCols[i] = ""
		} else {
			resultCols[i] = switchType(val)
		}
	}
	return resultCols
}

func switchType(val interface{}) string {
	var result string
	switch val.(type) {
	case int, int32, int64, float64:
		result = fmt.Sprintf("%v", val)
	case *ora.Lob:
		newVal, ok := val.(*ora.Lob)
		if ok && newVal.Reader != nil {
			b, err := ioutil.ReadAll(newVal)
			if err != nil {
				result = fmt.Sprintf("%v", err)
			} else {
				result = string(b)
			}
		} else {
			result = ""
		}
		newVal.Close()
	default:
		result = fmt.Sprintf("%s", val)
	}
	return result
}
