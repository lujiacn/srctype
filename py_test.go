package srctype

import (
	"fmt"
	"testing"
)

func TestPyConn(t *testing.T) {
	connstr := "localhost:50060"
	studyCode := "TEST"
	dtSrcId := "adfasdfasdfadsf"
	script := `
import altair as alt
from vega_datasets import data
source = data.iris()
dataframe_output= source
	`
	c, err := NewPyConn(connstr, script, nil, studyCode, dtSrcId)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(c)
}
