package db

import "testing"

func TestParsePlaceHolderSqlToRaws(t *testing.T) {
	sql := "select * from orders where uid = ?"
	var args []interface{}
	args = append(args, 1)
	sqls, err := ParsePlaceHolderSqlToRaws(sql, args)
	if err != nil {
		t.Errorf("parse place holder sql to raws fail,err: %s", err.Error())
	}
	t.Log(sqls)
}
