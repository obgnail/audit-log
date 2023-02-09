package audit_log

import (
	"fmt"
	"github.com/obgnail/audit-log/types"
)

type Handler interface {
	OnAuditLog(auditLog *types.AuditLog) error
}

type FunctionHandler func(auditLog *types.AuditLog) error

func (f FunctionHandler) OnAuditLog(auditLog *types.AuditLog) error {
	return f(auditLog)
}

type DummyAuditLogHandler func(auditLog *types.AuditLog) error

func (f DummyAuditLogHandler) OnAuditLog(auditLog *types.AuditLog) error {
	fmt.Printf("get audit log: %+v", auditLog)
	return nil
}
