package compose

import (
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/canal"
	"github.com/obgnail/audit-log/compose/clickhouse"
	"github.com/obgnail/audit-log/compose/kafka"
	"github.com/obgnail/audit-log/compose/mysql"
	"log"
)

func init() {
	onStart(kafka.InitKafka)
	onStart(kafka.InitKafkaOffset)
	onStart(canal.InitCanal)
	onStart(clickhouse.InitClickHouse)
	onStart(mysql.InitMySQL)
}

func onStart(fn func() error) {
	if err := fn(); err != nil {
		log.Fatalf("Error at onStart: %s\n", errors.ErrorStack(err))
	}
}

//type DBContext struct {
//	sqlDb *gorp.DbMap
//	sqlTx *gorp.Transaction
//	ctx   context.Context
//}
//
//func newDBContext(ctx context.Context, db gorp.SqlExecutor) *DBContext {
//	dbc := &DBContext{
//		ctx: ctx,
//	}
//	switch db.(type) {
//	case *gorp.DbMap:
//		dbc.sqlDb = db.(*gorp.DbMap)
//	case *gorp.Transaction:
//		dbc.sqlTx = db.(*gorp.Transaction)
//		dbc.sqlDb = TransactionDB(dbc.sqlTx)
//	default:
//		panic(fmt.Sprintf("unsupported gorp.SqlExecutor: %v", db))
//	}
//	return dbc
//}
//
//func NewDBContext(src gorp.SqlExecutor) *DBContext {
//	return newDBContext(context.TODO(), src)
//}
//
//func TransactionDB(tx *gorp.Transaction) *gorp.DbMap {
//	rtx := reflect.Indirect(reflect.ValueOf(tx))
//	rdbm := rtx.FieldByName("dbmap")
//	rpdbm := unsafe.Pointer(rdbm.UnsafeAddr())
//	pdbm := (**gorp.DbMap)(rpdbm)
//	return *pdbm
//}
