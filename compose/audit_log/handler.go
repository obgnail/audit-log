package audit_log

import (
	"fmt"
	"github.com/obgnail/audit-log/compose/clickhouse"
)

type Handler interface {
	OnAuditLog(auditLog clickhouse.TxInfoBinlogEvent) error
}

type FunctionHandler func(auditLog clickhouse.TxInfoBinlogEvent) error

func (f FunctionHandler) OnAuditLog(auditLog clickhouse.TxInfoBinlogEvent) error {
	return f(auditLog)
}

type DummyAuditLogHandler func(auditLog clickhouse.TxInfoBinlogEvent) error

func (f DummyAuditLogHandler) OnAuditLog(auditLog clickhouse.TxInfoBinlogEvent) error {
	fmt.Printf("get audit log: %+v", auditLog)
	return nil
}
