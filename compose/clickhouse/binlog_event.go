package clickhouse

import (
	"context"
	"github.com/obgnail/audit-log/compose/common"
	"time"

	"github.com/juju/errors"
)

type TxInfoBinlogEvent struct {
	Time         time.Time `ch:"time"`
	Context      string    `ch:"context"`
	GTID         string    `ch:"gtid"`
	BinlogEvents []BinlogEvent
}

func CombineTxAndEvents(txInfo TxInfo, events []BinlogEvent) TxInfoBinlogEvent {
	txBinlogEvent := TxInfoBinlogEvent{
		Time:         txInfo.Time,
		Context:      txInfo.Context,
		GTID:         txInfo.GTID,
		BinlogEvents: events,
	}
	return txBinlogEvent
}

type BinlogEvent struct {
	Db     string `ch:"db"`
	Table  string `ch:"table"`
	Action int32  `ch:"action"`
	GTID   string `ch:"gtid"`
	Data   string `ch:"data"`
}

func ConvertCHFormatBinlogEvent(binlog *common.BinlogEvent) BinlogEvent {
	return BinlogEvent{
		Db:     binlog.Db,
		Table:  binlog.Table,
		Action: int32(binlog.Action),
		GTID:   binlog.GTID,
		Data:   string(binlog.Data),
	}
}

func ListBinlogEvent(gtid string) ([]BinlogEvent, error) {
	var result []BinlogEvent
	s := "SELECT db, table, action, data, gtid FROM binlog_event WHERE gtid=$1;"
	err := CH.Select(context.Background(), &result, s, gtid)
	return result, errors.Trace(err)
}

func ListBinlogEvents(gtids []string) ([]BinlogEvent, error) {
	var result []BinlogEvent
	s := "SELECT db, table, action, data, gtid FROM binlog_event WHERE gtid IN ($1);"
	err := CH.Select(context.Background(), &result, s, gtids)
	return result, errors.Trace(err)
}

func InsertBinlogEvents(binlogEvents []BinlogEvent) error {
	length := len(binlogEvents)
	if length == 0 {
		return nil
	}
	batch, err := CH.PrepareBatch(context.Background(),
		"INSERT INTO binlog_event (db, table, action, gtid, data) VALUES")
	if err != nil {
		return errors.Trace(err)
	}
	var (
		dbs    = make([]string, length)
		tables = make([]string, length)
		action = make([]int32, length)
		GTIDs  = make([]string, length)
		events = make([]string, length)
	)
	for i, event := range binlogEvents {
		dbs[i] = event.Db
		tables[i] = event.Table
		action[i] = event.Action
		GTIDs[i] = event.GTID
		events[i] = event.Data
	}
	if err := batch.Column(0).Append(dbs); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(1).Append(tables); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(2).Append(action); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(3).Append(GTIDs); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(4).Append(events); err != nil {
		return errors.Trace(err)
	}

	if err = batch.Send(); err != nil {
		return errors.Trace(err)
	}
	return nil
}
