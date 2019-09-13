package srctype

import (
	"context"
	"encoding/csv"
	"fmt"
	"strings"

	pb "github.com/lujiacn/pygrpc"
	"google.golang.org/grpc"
)

type PyType struct {
	conn      *grpc.ClientConn
	client    pb.PygrpcClient
	colNames  []string
	result    [][]string
	rawStr    string
	context   context.Context
	cancel    context.CancelFunc
	nameSpace string
}

// NewPyConn initiate and run python script
func NewPyConn(connStr, script string, argData map[string]string, studyCode string, dtSrcId string) (Connector, error) {
	nameSpace := studyCode + dtSrcId
	maxMsgSize := 1024 * 1024 * 1024
	conn, err := grpc.Dial(connStr, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := pb.NewPygrpcClient(conn)
	ctx, cancel := context.WithCancel(context.Background())

	pyType := PyType{conn: conn, client: client, context: ctx, cancel: cancel, nameSpace: nameSpace}
	defer pyType.Close()
	// init
	client.PyNew(ctx, &pb.PyArgs{NameSpace: nameSpace})

	// assign

	for k, v := range argData {
		err := pyType.batchStrAssign(k, v)
		if err != nil {
			return nil, err
		}
	}

	// run script

	postScript := `
import json
output_grpc={}
output_grpc["colNames"]=""
output_grpc["dataStr"]=""
output_grpc["outStr"]=""
if 'dataframe_output' in locals():
    colNames = "|".join(dataframe_output.columns.values)
    dataStr = dataframe_output.to_csv(index=False)
    output_grpc["colNames"] = colNames
    output_grpc["dataStr"] = dataStr

if 'string_output' in locals():
    output_grpc["outStr"]= string_output
result_grpc = output_grpc
`
	script = script + postScript

	reply, err := client.PyExec(ctx, &pb.PyRequest{NameSpace: nameSpace, EvalVar: "result_grpc", Script: script})
	if err != nil {
		fmt.Println("Error", err)
		return nil, err
	}
	if !reply.Finished {
		message := reply.Message
		return nil, fmt.Errorf("%s", message)
	}

	// try get output colNames
	output := reply.GetEvalValue()
	if colNames, found := output["colNames"]; found {
		pyType.colNames = strings.Split(colNames, "|")
	}

	if dataframe, found := output["dataStr"]; found {
		csvReader := csv.NewReader(strings.NewReader(dataframe))
		allData, err := csvReader.ReadAll()
		if err != nil {
			return nil, err
		}

		if len(allData) > 0 {
			pyType.result = allData[1:len(allData)]
		}

	}

	if outstring, found := output["outStr"]; found {
		pyType.rawStr = outstring
	}

	return &pyType, nil
}

func (m *PyType) batchStrAssign(rVar, rawData string) (err error) {
	batchSize := 1024*1024*3 - 1
	//if len(rawData) > batchSize {
	//return fmt.Errorf("Data Source too large %v M(> 4M)!", len(rawData)/(1024*1024))
	//}

	addRound := 0
	tLen := len(rawData)
	if tLen%batchSize > 0 {
		addRound = 1
	}

	batchNumber := (tLen / batchSize) + addRound
	//client := pb.NewPygrpcClient(m.conn)
	for i := 0; i < batchNumber; i++ {
		var value string
		if i == batchNumber-1 {
			value = rawData[i*batchSize : i*batchSize+tLen%batchSize]
		} else {
			value = rawData[i*batchSize : i*batchSize+batchSize]
		}
		reply, err := m.client.PyAssign(m.context, &pb.PyArgs{NameSpace: m.nameSpace, ArgName: rVar, ArgValue: []byte(value)})
		if err != nil {
			return err
		}
		if reply.Finished != true {
			message := reply.Message
			return fmt.Errorf("%s", message)
		}
	}
	// conver to data frame
	// file name is argName_file
	script := fmt.Sprintf(`
import pandas as pd
%s = pd.read_csv(%s_file, dtype=str)

import os
os.remove(%s_file)
`, rVar, rVar, rVar)
	_, err = m.client.PyExec(m.context, &pb.PyRequest{NameSpace: m.nameSpace, Script: script})
	if err != nil {
		return err
	}

	return nil

}

func (m *PyType) ColNames() ([]string, error) {
	return m.colNames, nil
}

func (m *PyType) Close() {
	m.client.PyClose(m.context, &pb.PyArgs{NameSpace: m.nameSpace})
	m.conn.Close()
	return
}

func (m *PyType) ReadAll() ([][]string, error) {
	return m.result, nil
}

func (m *PyType) Read() ([]string, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *PyType) ReadStr() (string, error) {
	return m.rawStr, nil
}

func (c *PyType) ReadRowToChan(chan interface{}) {
	panic("not implemented")
	return
}
