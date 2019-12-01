package db

import (
	"context"
	"errors"
	"github.com/didi/gendry/builder"
	"github.com/didi/gendry/scanner"
	"github.com/gin-gonic/gin"
)

type Base struct {
	DBName    string
	TableName string
	IsMaster  bool
	Columns   []interface{}
	Db        *DB
}

//获取对于table对应的列
func (base *Base) GetColumnsFromMysqlTable() (map[string]map[string]string, error) {
	if base.Db == nil {
		//todo init
		return nil, errors.New("Db not be inited")
	}
	// Store colum as map of maps
	columnDataTypes := make(map[string]map[string]string)
	// Select columnd data from INFORMATION_SCHEMA
	columnDataTypeQuery := "SELECT COLUMN_NAME, COLUMN_KEY, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ? and  TABLE_SCHEMA = ?"
	rows, err := base.Db.Query(context.Background(), columnDataTypeQuery, base.TableName, base.DBName)
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

func (base *Base) GetSubList(c *gin.Context, where map[string]interface{}, fields []string) ([]map[string]interface{}, error) {
	emptySet := []map[string]interface{}{}
	if base.Db == nil {
		return emptySet, errors.New("Db is nil")
	}
	cond, value, err := builder.BuildSelect(base.TableName, where, fields)
	if err != nil {
		return emptySet, err
	}
	if base.IsMaster {
		base.Db.SetMaster() //todo 是否会影响其他
	}
	ctx := context.Background()
	rows, err := base.Db.Query(ctx, cond, value)
	if err != nil || nil == rows.Rows {
		return emptySet, err
	}
	defer rows.Rows.Close()
	return scanner.ScanMapDecode(rows.Rows)
}

func (base *Base) GetList(c *gin.Context, where map[string]interface{}) ([]map[string]interface{}, error) {
	emptySet := []map[string]interface{}{}
	if base.Db == nil {
		return emptySet, errors.New("Db is nil")
	}
	cond, values, err := builder.BuildSelect(base.TableName, where, nil)
	if err != nil {
		return emptySet, err
	}
	if base.IsMaster {
		base.Db.SetMaster()
	}
	ctx := context.Background()
	rows, err := base.Db.Query(ctx, cond, values)
	if err != nil || nil == rows.Rows {
		return emptySet, err
	}
	defer rows.Rows.Close()
	return scanner.ScanMapDecode(rows.Rows)
}
func (base *Base) GetItem(c *gin.Context, where map[string]interface{}) (map[string]interface{}, error) {
	emptyRet := make(map[string]interface{})
	if base.Db == nil {
		return emptyRet, errors.New("Db is nil")
	}
	rows, err := base.GetList(c, where)
	if err != nil {
		return emptyRet, err
	}
	return rows[0], nil
}
func (base *Base) GetItemByUK(c *gin.Context, field string, value interface{}) (map[string]interface{}, error) {
	where := make(map[string]interface{})
	where[field] = value
	return base.GetItem(c, where)
}
func (base *Base) GetItemByPK(c *gin.Context, value interface{}) (map[string]interface{}, error) {
	return base.GetItemByUK(c, "id", value)
}
func (base *Base) Insert(c *gin.Context, data []map[string]interface{}) (int64, error) {
	if base.Db == nil {
		return 0, errors.New("Db is nil")
	}
	cond, values, err := builder.BuildInsert(base.TableName, data)
	if err != nil {
		return 0, err
	}
	result, err := base.Db.Exec(context.Background(), cond, values)
	if err != nil || nil == result {
		return 0, err
	}
	return result.LastInsertId()
}
func (base *Base) Update(c *gin.Context, where, data map[string]interface{}) (int64, error) {
	if base.Db == nil {
		return 0, errors.New("db is nil")
	}
	cond, values, err := builder.BuildUpdate(base.TableName, where, data)
	if err != nil {
		return 0, err
	}
	result, err := base.Db.Exec(context.Background(), cond, values)
	if err != nil || nil == result {
		return 0, err
	}
	return result.RowsAffected()
}

func (base *Base) Save(c *gin.Context, data map[string]interface{}, insertWithId bool) (int64, error) {
	_, ok := data["id"]
	if ok && insertWithId {
		mp := []map[string]interface{}{}
		mp[0] = data
		return base.Insert(c, mp)
	} else {
		if ok {
			delete(data, "id")
		}
		return base.Update(c, data, data)
	}
}
