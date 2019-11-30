package db

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
)

var (
	// ErrStmtNil prepared stmt error
	ErrStmtNil = errors.New("sql: prepare failed and stmt nil")
	// ErrNoMaster is returned by Master when call master multiple times.
	ErrNoMaster = errors.New("sql: no master instance")
	// ErrNoRows is returned by Scan when QueryRow doesn't return a row.
	// In such a case, QueryRow returns a placeholder *Row value that defers
	// this error until a Scan.
	ErrNoRows = sql.ErrNoRows
	// ErrTxDone transaction done.
	ErrTxDone = sql.ErrTxDone
)

type DB struct {
	write            []*conn
	read             []*conn
	serviceTimeStamp uint32
	dbSet            []string
	master           bool //todo 放哪里
	idx              int64
}

type conn struct {
	*sql.DB
	status int
}

type Tx struct {
	db     *conn
	tx     *sql.Tx
	c      context.Context
	cancel func()
}
type Row struct {
	err error
	*sql.Row
	db     *conn
	query  string
	args   []interface{}
	cancel func()
}

func (r *Row) Scan(dest ...interface{}) (err error) {
	if r.err != nil {
		err = r.err
	} else if r.Row == nil {
		err = ErrNoRows
	}
	if err != nil {
		return
	}
	err = r.Row.Scan(dest...)
	if r.cancel != nil {
		r.cancel()
	}
	if err != ErrNoRows {
		//err = errors.Wrapf(err, "query %s args %+v", r.query, r.args)
	}
	return
}

type Rows struct {
	*sql.Rows
	cancel func()
}

//Close closes the Rows, preventing further enumeration. If Next is called
// and returns false and there are no further result sets,
// the Rows are closed automatically and it will suffice to check the
// result of Err. Close is idempotent and does not affect the result of Err.
func (rs *Rows) Close(err error) {
	err = rs.Rows.Close()
	if rs.cancel != nil {
		rs.cancel()
	}
	return
}

type Stmt struct {
	stmt  *sql.Stmt //todo
	db    *conn
	tx    bool
	query string
	//stmt atomic.Value
}

func (db *DB) Begin(c context.Context) (tx *Tx, err error) {
	//todo read master
	//master是个数组???
	return db.write[0].begin(c)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) Exec(c context.Context, query string, args ...interface{}) (res sql.Result, err error) {
	//todo 选择其中一个master实例执行
	return db.write[0].Exec(query, args)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
func (db *DB) Prepare(query string) (*Stmt, error) {
	////todo 选择其中一个master实例执行
	return db.write[0].prepare(query)
}

// Query executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
func (db *DB) Query(c context.Context, query string, args ...interface{}) (rows *Rows, err error) {
	idx := db.readIndex()
	for i := range db.read {
		if rows, err = db.read[(idx+i)%len(db.read)].query(c, query, args...); err == nil {
			return
		}
	}
	return db.write[0].query(c, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until Row's
// Scan method is called.
func (db *DB) QueryRow(c context.Context, query string, args ...interface{}) *Row {
	idx := db.readIndex()
	for i := range db.read {
		if row := db.read[(idx+i)%len(db.read)].queryRow(c, query, args...); err == nil {
			return row
		}
	}
	return db.write[0].queryRow(c, query, args...)
}

func (db *DB) Close() (err error) {
	for _, wd := range db.write {
		if e := wd.DB.Close(); e != nil {
			err = e
		}
	}
	for _, wd := range db.read {
		if e := wd.DB.Close(); e != nil {
			err = e
		}
	}
	return
}
func (db *DB) readIndex() int {
	if len(db.read) == 0 {
		return 0
	}
	v := atomic.AddInt64(&db.idx, 1)
	return int(v) % len(db.read)
}
func (conn *conn) begin(ctx context.Context) (tx *Tx, err error) {
	//添加事务超时时间
	//事务超时取消
	rtx, err := conn.DB.BeginTx(ctx, nil)
	//添加统计stat数据
	if err != nil {
		return
	}
	tx = &Tx{tx: rtx, db: conn, c: ctx}
	return
}

func (conn *conn) prepare(query string) (*Stmt, error) {
	stmt, err := conn.DB.Prepare(query)
	if err != nil {
		return nil, err
	}
	st := &Stmt{stmt: stmt, query: query, db: conn}
	return st, nil
}

func (conn *conn) query(c context.Context, query string, args ...interface{}) (*Rows, error) {
	rs, err := conn.DB.QueryContext(c, query, args)
	if err != nil {
		return nil, err
	}
	//todo 添加统计信息，超时取消等
	return &Rows{rs, nil}, nil
}

func (conn *conn) queryRow(c context.Context, query string, args ...interface{}) *Row {
	r := conn.DB.QueryRowContext(c, query, args)
	//todo 统计信息，超时取消等待
	return &Row{db: conn, Row: r, query: query, args: args}
}

func (s *Stmt) Close() (err error) {
	if s == nil {
		err = ErrStmtNil
		return
	}
	//load atomic vale
	if s.stmt == nil {
		err = ErrStmtNil
		return
	}
	err = s.stmt.Close()
	return
}

// Exec executes a prepared statement with the given arguments and returns a
// Result summarizing the effect of the statement.
func (s *Stmt) Exec(c context.Context, args ...interface{}) (res sql.Result, err error) {
	if s == nil || s.stmt == nil {
		err = ErrStmtNil
		return
	}
	//add static info
	//cancel func
	res, err = s.stmt.ExecContext(c, args...)
	/*if err != nil {
		err = errors.Wrapf(err, "exec:%s, args:%+v", s.query, args)
	   //"github.com/pkg/errors"
	}*/
	return
}

// Query executes a prepared query statement with the given arguments and
// returns the query results as a *Rows.
func (s *Stmt) Query(c context.Context, args ...interface{}) (rows *Rows, err error) {
	if s == nil || s.stmt == nil {
		err = ErrStmtNil
		return
	}
	//add static info
	//cancel func
	rs, err := s.stmt.QueryContext(c, args...)
	if err != nil {
		return
	}
	rows = &Rows{rs, nil}
	return
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error will
// be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the rest.
func (s *Stmt) QueryRow(c context.Context, args ...interface{}) (row *Row) {
	row = &Row{db: s.db, query: s.query, args: args}
	if s == nil || s.stmt == nil {
		row.err = ErrStmtNil
		return
	}
	//add static info
	//cancel func
	row.Row = s.stmt.QueryRow(args...)
	row.cancel = nil
	return
}

// Commit commits the transaction.
func (tx *Tx) Commit() (err error) {
	err = tx.tx.Commit()
	if tx.cancel != nil {
		tx.cancel()
	}
	if err != nil {
		//err = errors.WithStack(err)
	}
	return
}

func (tx *Tx) RollBack(err error) {
	err = tx.tx.Rollback()
	if tx.cancel != nil {
		tx.cancel()
	}
	if err != nil {
		//err = errors.WithStack(err)
	}
	return
}

// Exec executes a query that doesn't return rows. For example: an INSERT and UPDATE.
func (tx *Tx) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	res, err = tx.tx.ExecContext(tx.c, query, args...)
	if err != nil {
		//err = errors.Wrapf(err, "exec:%s, args:%+v", query, args)
	}
	return
}

// Query executes a query that returns rows, typically a SELECT.
func (tx *Tx) Query(query string, args ...interface{}) (rows *Rows, err error) {
	rs, err := tx.tx.QueryContext(tx.c, query, args...)
	if err == nil {
		rows = &Rows{Rows: rs}
	} else {
		//err = errors.Wrapf(err, "query:%s, args:%+v", query, args)
	}
	return
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until Row's
// Scan method is called.
func (tx *Tx) QueryRow(query string, args ...interface{}) *Row {
	r := tx.tx.QueryRowContext(tx.c, query, args...)
	return &Row{Row: r, db: tx.db, query: query, args: args}
}

// Stmt returns a transaction-specific prepared statement from an existing statement.
func (tx *Tx) Stmt(stmt *Stmt) *Stmt {
	if stmt == nil || stmt.stmt == nil {
		return nil
	}
	_ = tx.tx.StmtContext(tx.c, stmt.stmt)
	st := &Stmt{query: stmt.query, tx: true, db: tx.db}
	return st
}

// Prepare creates a prepared statement for use within a transaction.
// The returned statement operates within the transaction and can no longer be
// used once the transaction has been committed or rolled back.
// To use an existing prepared statement on this transaction, see Tx.Stmt.
func (tx *Tx) Prepare(query string) (*Stmt, error) {
	stmt, err := tx.tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	st := &Stmt{stmt: stmt, query: query, tx: true, db: tx.db}
	return st, nil
}
