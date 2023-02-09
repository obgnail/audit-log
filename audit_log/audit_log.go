package audit_log

import (
	"github.com/obgnail/audit-log/clickhouse"
	"github.com/obgnail/audit-log/config"
	"github.com/obgnail/audit-log/logger"
	"github.com/obgnail/audit-log/mysql"
	"github.com/obgnail/audit-log/syncer"
)

type AuditLogger struct {
	binlogSyncer *syncer.BinlogSynchronizer
	txInfoSyncer *syncer.TxInfoSynchronizer
}

func New(binlogSyncer *syncer.BinlogSynchronizer, txInfoSyncer *syncer.TxInfoSynchronizer) *AuditLogger {
	return &AuditLogger{binlogSyncer: binlogSyncer, txInfoSyncer: txInfoSyncer}
}

func (log *AuditLogger) Sync(handler Handler) {
	go log.txInfoSyncer.HandleAuditLog(handler.OnAuditLog)
	log.binlogSyncer.Sync()
	log.txInfoSyncer.Sync()
}

func Init(path string) {
	checkErr(config.InitConfig(path))
	checkErr(logger.InitLogger())
	checkErr(syncer.InitBinlogSyncer())
	checkErr(syncer.InitTxInfoSyncer())
	checkErr(mysql.InitDBM())
	checkErr(clickhouse.InitClickHouse())
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Run(handler Handler) {
	New(syncer.BinlogSyncer, syncer.TxInfoSyncer).Sync(handler)
}
