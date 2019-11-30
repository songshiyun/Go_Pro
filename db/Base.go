package db

import (
	"context"
	"errors"
)

type Base struct {
	DBName    string
	TableName string
	IsMaster  bool
	Columns   []interface{}
	db        *DB
}

func (base *Base) GetColumnsFromMysqlTable() (map[string]map[string]string, error) {
	if base.db == nil {
		//todo init
	}
	// Store colum as map of maps
	columnDataTypes := make(map[string]map[string]string)
	// Select columnd data from INFORMATION_SCHEMA
	columnDataTypeQuery := "SELECT COLUMN_NAME, COLUMN_KEY, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND table_name = ?"
	rows, err := base.db.Query(context.Background(), columnDataTypeQuery, base.DBName, base.TableName)
	if err != nil {
		return nil, err
	}
	if rows == nil || rows.Rows == nil {
		return nil, errors.New("empty rows")
	}
	rs := rows.Rows
	defer rs.Close()
	for rs.Next() {
		var colunm string
		var colunmKey string
		var dataType string
		var nullAble string
		_ = rs.Scan(&colunm, &colunmKey, &dataType, &nullAble)
		columnDataTypes[colunm] = map[string]string{"value": dataType, "nullable": nullAble, "primary": colunmKey}
	}
	return columnDataTypes, nil
}
