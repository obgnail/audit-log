package compose

import (
	"context"
	"fmt"
	"gopkg.in/gorp.v1"
	"reflect"
	"unsafe"
)

type DBContext struct {
	sqlDb *gorp.DbMap
	sqlTx *gorp.Transaction
	ctx   context.Context
}

func newDBContext(ctx context.Context, db gorp.SqlExecutor) *DBContext {
	dbc := &DBContext{
		ctx: ctx,
	}
	switch db.(type) {
	case *gorp.DbMap:
		dbc.sqlDb = db.(*gorp.DbMap)
	case *gorp.Transaction:
		dbc.sqlTx = db.(*gorp.Transaction)
		dbc.sqlDb = TransactionDB(dbc.sqlTx)
	default:
		panic(fmt.Sprintf("unsupported gorp.SqlExecutor: %v", db))
	}
	return dbc
}

func NewDBContext(src gorp.SqlExecutor) *DBContext {
	return newDBContext(context.TODO(), src)
}

func TransactionDB(tx *gorp.Transaction) *gorp.DbMap {
	rtx := reflect.Indirect(reflect.ValueOf(tx))
	rdbm := rtx.FieldByName("dbmap")
	rpdbm := unsafe.Pointer(rdbm.UnsafeAddr())
	pdbm := (**gorp.DbMap)(rpdbm)
	return *pdbm
}
