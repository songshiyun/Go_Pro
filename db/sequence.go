package db

import (
	"errors"
	"fmt"
	"math/rand"
)

type SequencerFun func(dbName, tableName string) (id int64, err error)

type Sequencer struct {
	Id   int64
	Name string
	f    SequencerFun
}

func (se *Sequencer) GenerateSequenceID() {
	var seq Sequencer
	seq.Id = int64(rand.Intn(100))
	seq.Name = "test"
	seq.f = func(dbName, tableName string) (id int64, err error) {
		fmt.Println("db name: " + dbName)
		fmt.Println("table name: " + tableName)
		return int64(rand.Intn(1000)), nil
	}
}

type IdGenerator interface {
	GenerateID(params ...interface{}) (int64, error)
}

type MysqlIDGenerator struct {
}

func (mysqlIDGen *MysqlIDGenerator) GenerateID(params ...interface{}) (int64, error) {
	if len(params) != 2 {
		return 0, errors.New("bad params for mysql generator")
	}
	return rand.Int63n(int64(rand.Intn(1000))), nil
}
