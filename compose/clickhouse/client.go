package clickhouse

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/obgnail/audit-log/audit_log"
	"github.com/pingcap/errors"
	"time"
)

func InsertTxInfo(CH clickhouse.Conn, txInfos []*audit_log.TxInfo) error {
	length := len(txInfos)
	if length == 0 {
		return nil
	}
	batch, err := CH.PrepareBatch(context.Background(), "INSERT INTO tx_info (gtid, context, user_uuid, tx_time, status) VALUES")
	if err != nil {
		return errors.Trace(err)
	}
	var (
		gtids     = make([]string, length)
		contexts  = make([]string, length)
		userUUIDs = make([]string, length)
		txTimes   = make([]time.Time, length)
	)
	for i, info := range txInfos {
		gtids[i] = info.GTID
		contexts[i] = info.Context
		userUUIDs[i] = info.UserUUID
		txTimes[i] = info.TxTime
	}
	if err := batch.Column(0).Append(gtids); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(1).Append(contexts); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(2).Append(userUUIDs); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(3).Append(txTimes); err != nil {
		return errors.Trace(err)
	}
	err = batch.Send()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func InsertBinlogEvents(CH clickhouse.Conn, binlogEvents []*audit_log.BinlogEvent) error {
	length := len(binlogEvents)
	if length == 0 {
		return nil
	}
	batch, err := CH.PrepareBatch(context.Background(), "INSERT INTO binlog_event (event_db, event_table, event_action, gtid, event) VALUES")
	if err != nil {
		return errors.Trace(err)
	}
	var (
		dbs          = make([]string, length)
		tables       = make([]string, length)
		eventActions = make([]int, length)
		gtids        = make([]string, length)
		events       = make([]string, length)
	)
	for i, be := range binlogEvents {
		dbs[i] = be.Db
		tables[i] = be.EventTable
		eventActions[i] = be.EventAction
		gtids[i] = be.GTID
		events[i] = string(be.Event)
	}
	if err := batch.Column(0).Append(dbs); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(1).Append(tables); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(2).Append(eventActions); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(3).Append(gtids); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(4).Append(events); err != nil {
		return errors.Trace(err)
	}
	err = batch.Send()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

const (
	timeLayout = "2006-01-02 15:04:05.000"
)

type RecordCount struct {
	Count uint64 `ch:"count"`
	GTID  string `ch:"gtid"`
}

func AuditLogExist(CH clickhouse.Conn, txTime time.Time, gtid string) (bool, error) {
	s := "SELECT count(1) AS count FROM audit_log WHERE tx_time=toDateTime($1, 3, 'Asia/Shanghai') AND gtid=$2;"
	c := make([]RecordCount, 0)
	err := CH.Select(context.Background(), &c, s, txTime.Format(timeLayout), gtid)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(c) > 0 && c[0].Count > 0 {
		return true, nil
	}
	return false, nil
}

func ListAuditLogs(CH clickhouse.Conn, txTime time.Time, gtid []string) (map[string]struct{}, error) {
	s := "SELECT count(1) AS count, gtid FROM audit_log WHERE tx_time>=toDateTime($1, 3, 'Asia/Shanghai') AND gtid IN ($2) GROUP BY gtid;"
	rc := make([]RecordCount, 0)
	err := CH.Select(context.Background(), &rc, s, txTime.Format(timeLayout), gtid)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result := make(map[string]struct{})
	for _, c := range rc {
		if c.Count > 0 {
			result[c.GTID] = struct{}{}
		}
	}
	return result, nil
}

func ListTxBinlogEvent(CH clickhouse.Conn, gtid string) ([]audit_log.BinlogEvent, error) {
	var result []audit_log.BinlogEvent
	s := "SELECT event_db AS db, event_table AS table, event_action AS action, event, gtid FROM binlog_event WHERE gtid=$1;"
	err := CH.Select(context.Background(), &result, s, gtid)
	return result, errors.Trace(err)
}

func ListTxBinlogEvents(CH clickhouse.Conn, gtids []string) ([]audit_log.BinlogEvent, error) {
	var result []audit_log.BinlogEvent
	s := "SELECT event_db AS db, event_table AS table, event_action AS action, event, gtid FROM binlog_event WHERE gtid IN ($1);"
	err := CH.Select(context.Background(), &result, s, gtids)
	return result, errors.Trace(err)
}
