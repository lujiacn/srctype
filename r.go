package srctype

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"gopkg.in/lujiacn/rservcli.v0"
)

//RSrouce for r file type
type rType struct {
	rClient *rservcli.Rcli
	rScript string
}

//NewRPortal create instance for R script which return dataframe
func NewRTypeConn(connStr, rScript string,
	argData map[string]string) (Connector, error) {
	host, port, err := parseConnStr(connStr)
	if err != nil {
		return nil, err
	}

	rClient, err := rservcli.NewRcli(host, port)
	if err != nil {
		return nil, err
	}
	r := &rType{rClient: rClient, rScript: rScript}
	err = r.init(argData)
	if err != nil {
		return r, err
	}
	return r, nil
}

// batchStrAssign do string assignment by each 16M
func batchStrAssign(rVar, rawData string, rClient *rservcli.Rcli) (err error) {
	batchSize := 16*1024*1024 - 1
	addRound := 0
	tLen := len(rawData)
	if tLen%batchSize > 0 {
		addRound = 1
	}
	//debug
	// fmt.Printf("R raw string length is %v \n", tLen)
	// fmt.Printf("tlen devide batchSize is %v", tLen/batchSize)

	batchNumber := (tLen / batchSize) + addRound
	batchRvar := []string{}
	for i := 0; i < batchNumber; i++ {
		var value string
		if i == batchNumber-1 {
			value = rawData[i*batchSize : i*batchSize+tLen%batchSize]
		} else {
			value = rawData[i*batchSize : i*batchSize+batchSize]
		}

		tRvar := fmt.Sprintf("%s_%v", rVar, i)
		err = rClient.Assign(tRvar, value)
		if err != nil {
			return err
		}
		batchRvar = append(batchRvar, tRvar)
	}

	//merg in R
	tScript := fmt.Sprintf("%s <- paste0(%s)", rVar, strings.Join(batchRvar, ", "))
	err = rClient.VoidExec(tScript)
	if err != nil {
		return err
	}
	return nil

}

func (r *rType) argsAssign(argData map[string]string) (err error) {
	for key, item := range argData {
		err = batchStrAssign(fmt.Sprintf("%s_str", key), item, r.rClient)
		if err != nil {
			return err
		}

		rScript := fmt.Sprintf(`
		library(dplyr)
		%s <- read.csv(text=%s_str, header=TRUE, stringsAsFactors=FALSE)
		%s <- %s %%>%% mutate_if(is.numeric, as.character)
		`, key, key, key, key)

		err = r.rClient.VoidExec(rScript)
		if err != nil {
			errStr := fmt.Sprintf("Error during assignment: %v", err)
			return errors.New(errStr)
		}
	}
	return nil
}

func (r *rType) init(argData map[string]string) (err error) {
	//args Assignment
	err = r.argsAssign(argData)
	if err != nil {
		return err
	}

	err = r.rClient.VoidExec(r.rScript)
	if err != nil {
		return err
	}

	// do iterator
	iScript := `if (exists("dataframe_output")) {
		if(!require(iterators)){
			install.packages("iterators")
		}
		library(iterators)
		output_iter=iter(dataframe_output, by="row")
		}`
	err = r.rClient.VoidExec(iScript)

	if err != nil {
		return err
	}

	return nil
}

//RemoteRead() for dataframe returned R script
func (r *rType) Read() ([]string, error) {
	if r.rClient == nil {
		return nil, errors.New("No R connection")
	}

	out, err := r.rClient.Eval(`
	try(as.vector(unlist(nextElem(output_iter), use.names=FALSE)))
	`)
	if err != nil {
		return nil, err
	}
	switch out.(type) {
	case []string:
		return out.([]string), nil
	case string:
		if strings.Contains(out.(string), "Error : StopIteration") {
			return nil, io.EOF
		} else {
			return []string{out.(string)}, nil
		}
	}
	return nil, errors.New("unclear type")
}

//RemoteReadAll() for dataframe returned R script
func (r *rType) ReadAll() ([][]string, error) {
	defer r.Close()

	if r.rClient == nil {
		return nil, errors.New("No R connection")
	}

	addScript := `
	if (exists("dataframe_output")) {
		tempFileName <- tempfile()
		write.csv(dataframe_output, tempFileName, row.names=FALSE)
	}
	if (exists("filename_output")) {
		tempFileName <- filename_output
	}

	#read data
	t_out <- readLines(tempFileName)
	t_out
	`
	rawData, err := r.rClient.Eval(addScript)
	if err != nil {
		return nil, err
	}

	var rawDataStr string
	switch rawData.(type) {
	case []string:
		rawDataStr = strings.Join(rawData.([]string), "\n")
	case string:
		rawDataStr = rawData.(string)
	}
	csvReader := csv.NewReader(strings.NewReader(rawDataStr))
	allData, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(allData) > 0 {
		return allData[1:len(allData)], nil
	}
	return allData, nil
}

//RemoColNames() for dataframe returned R script
func (r *rType) ColNames() ([]string, error) {
	if r.rClient == nil {
		return nil, errors.New("No R connection")
	}
	script := `
	col_names <- colnames(dataframe_output)
	`
	colNames, err := r.rClient.Eval(script)
	if err != nil {
		return nil, err
	}
	switch colNames.(type) {
	case string:
		return []string{colNames.(string)}, nil
	case []string:
		return colNames.([]string), nil
	}
	return nil, errors.New("Unclear about col names.")
}

func (r *rType) Close() {
	if r.rClient != nil {
		r.rClient.Close()
	}
}

//RemoteString will run R script and read string_output
func (r *rType) ReadStr() (string, error) {
	addScript := `paste(string_output, collapse="")`

	rawData, err := r.rClient.Eval(addScript)
	if err != nil {
		return "", err
	}

	output := fmt.Sprintf("%v", rawData)
	return string(output), err
}

func parseConnStr(str string) (host string, port int64, err error) {
	list := strings.Split(str, ";")
	if host, found := list[0]; !found {
		err = errors.New("R connection string format error. Please use ip:port format.")
		return
	}
	if portStr, found := list[1]; !found {
		err = errors.New("R connection string format error. Please use ip:port format.")
		return
	}
	port, err = strconv.ParseInt(portStr, 10, 64)
	if err != nil {
		return
	}
	return
}
