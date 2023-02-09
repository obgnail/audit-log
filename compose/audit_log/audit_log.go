package audit_log

import (
	"github.com/obgnail/audit-log/compose/syncer"
)

type AuditLogger struct {
	*syncer.BinlogSynchronizer
	*syncer.TxInfoSynchronizer
}

func New(binlogSyncer *syncer.BinlogSynchronizer, txInfoSyncer *syncer.TxInfoSynchronizer) *AuditLogger {
	return &AuditLogger{BinlogSynchronizer: binlogSyncer, TxInfoSynchronizer: txInfoSyncer}
}

func (a *AuditLogger) Sync(handler Handler) {
	go a.TxInfoSynchronizer.HandleAuditLog(handler.OnAuditLog)
	a.BinlogSynchronizer.Sync()
	a.TxInfoSynchronizer.Sync()
}

func Run(handler Handler) {
	New(syncer.BinlogSyncer, syncer.TxInfoSyncer).Sync(handler)
}
