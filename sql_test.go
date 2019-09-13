package srctype

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/alexbrainman/odbc"
)

func TestTeraData(t *testing.T) {
	connStr := fmt.Sprintf("driver=%s;dbcname=%s;username=%s;password=%s",
		"/Library/Application Support/teradata/client/16.20/lib/tdataodbc_sbu.dylib",
		"teraprd", "U_CDH_DIVA_PRD", "CLINSTORE_DIVA_POC")
	db, err := sql.Open("odbc", connStr)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	query := "select * from CDH_BV_ODS_PRD.EFC15156_DM$LIVE"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		fmt.Println("error in rows", err)
		panic(err)
	}
	colNames, err := rows.Columns()
	fmt.Println("finnally", colNames, err)
}
