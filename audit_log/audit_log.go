package audit_log

import (
	"fmt"
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
	if err := config.InitConfig(path); err != nil {
		panic(err)
	}
	onStart(logger.InitLogger)
	onStart(syncer.InitBinlogSyncer)
	onStart(syncer.InitTxInfoSyncer)
	onStart(mysql.InitDBM)
	onStart(clickhouse.InitClickHouse)
}

func onStart(fn func() error) {
	if err := fn(); err != nil {
		panic(fmt.Sprintf("Error at onStart: %s\n", err))
	}
}

func Run(handler Handler) {
	New(syncer.BinlogSyncer, syncer.TxInfoSyncer).Sync(handler)
}
