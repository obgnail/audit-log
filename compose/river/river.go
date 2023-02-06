package river

import (
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/broker"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/audit-log/config"
	"github.com/obgnail/mysql-river/river"
	"strings"
	"time"
)

type River struct {
	include map[string]map[string]struct{} // map[db]map[table]struct{}
	broker.BinlogBroker
	broker.TxInfoBroker
}

func NewHandler(tables []string, binlogBroker broker.BinlogBroker, txInfoBroker broker.TxInfoBroker) *River {
	mapDb2Table := make(map[string]map[string]struct{})
	for _, ele := range tables {
		list := strings.Split(ele, ".")
		db, table := list[0], list[1]
		if _, ok := mapDb2Table[db]; !ok {
			mapDb2Table[db] = make(map[string]struct{})
		}
		mapDb2Table[db][table] = struct{}{}
	}
	h := &River{include: mapDb2Table, BinlogBroker: binlogBroker, TxInfoBroker: txInfoBroker}
	return h
}

func (h *River) String() string {
	return "audit log"
}

func (h *River) OnEvent(event *river.EventData) error {
	switch event.EventType {
	case river.EventTypeInsert, river.EventTypeUpdate, river.EventTypeDelete:
		if h.check(event.Db, event.Table) {
			binlog, err := common.NewBinlogEvent(event)
			if err != nil {
				return errors.Trace(err)
			}
			if err := h.PushLog(binlog); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (h *River) OnAlert(msg *river.StatusMsg) error {
	river.Logger.Warnf("on alert: %+v", *msg)
	return nil
}

func (h *River) OnClose(*river.River) {
	river.Logger.Error("audit log had done")
	return
}

func (h *River) check(db, table string) bool {
	if _, ok := h.include[db]; !ok {
		return false
	}
	if _, ok := h.include[db][table]; !ok {
		return false
	}
	return true
}

var (
	AuditLogRiver *River
)

func InitRiver() (err error) {
	_broker := broker.AuditLogBroker
	AuditLogRiver = NewHandler(config.AuditLog.HandleTables, _broker, _broker)
	return nil
}

func Run(from river.From) error {
	MySQL := config.MySQL
	PositionSaver := config.PositionSaver
	HealthChecker := config.HealthChecker
	cfg := &river.Config{
		MySQLConfig: &river.MySQLConfig{
			Host:     MySQL.Host,
			Port:     MySQL.Port,
			User:     MySQL.User,
			Password: MySQL.Password,
		},
		PosAutoSaverConfig: &river.PosAutoSaverConfig{
			SaveDir:      PositionSaver.SaveDir,
			SaveInterval: time.Duration(PositionSaver.SaveInterval) * time.Second,
		},
		HealthCheckerConfig: &river.HealthCheckerConfig{
			CheckPosThreshold: HealthChecker.CheckPosThreshold,
			CheckInterval:     time.Duration(HealthChecker.CheckInterval) * time.Second,
		},
	}
	handler := AuditLogRiver
	if err := river.New(cfg).SetHandler(handler).Sync(from); err != nil {
		return errors.Trace(err)
	}
	return nil
}
