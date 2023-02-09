package types

import (
	"context"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/clickhouse"
	"time"
)

const (
	StatusTxInfoUnprocessed = 1
	StatusTxInfoProcessed   = 2
)

const (
	timeLayout = "2006-01-02 15:04:05.000"
)

type ChTxInfo struct {
	Time    time.Time `json:"time" ch:"time"`
	Context string    `json:"context" ch:"context"`
	GTID    string    `json:"gtid" ch:"gtid"`
	Status  uint8     `json:"-" ch:"-"`
}

func InsertTxInfo(txInfo ChTxInfo) error {
	sql := "INSERT INTO tx_info (gtid, context, time, `status`) VALUES ($1, $2, $3, $4);"

	err := clickhouse.CH.Exec(context.Background(), sql,
		txInfo.GTID,
		txInfo.Context,
		txInfo.Time,
		txInfo.Status,
	)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func BatchInsertTxInfo(txInfoArr []ChTxInfo) error {
	length := len(txInfoArr)
	if length == 0 {
		return nil
	}

	batch, err := clickhouse.CH.PrepareBatch(context.Background(), "INSERT INTO tx_info VALUES")
	if err != nil {
		return errors.Trace(err)
	}

	gtidArr := make([]string, length)
	ctx := make([]string, length)
	TimeArr := make([]time.Time, length)
	statusArr := make([]uint8, length)

	for i := range txInfoArr {
		gtidArr[i] = txInfoArr[i].GTID
		ctx[i] = txInfoArr[i].Context
		TimeArr[i] = txInfoArr[i].Time
		statusArr[i] = txInfoArr[i].Status
	}

	if err := batch.Column(0).Append(gtidArr); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(1).Append(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(2).Append(TimeArr); err != nil {
		return errors.Trace(err)
	}
	if err := batch.Column(3).Append(statusArr); err != nil {
		return errors.Trace(err)
	}

	if err = batch.Send(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func ListUnprocessedTxInfo() ([]ChTxInfo, error) {
	sql := "SELECT gtid, context, time FROM tx_info " +
		"WHERE `status`=$1 AND time>=toDateTime64($2, 3) ORDER BY time DESC LIMIT 1000;"
	results := make([]ChTxInfo, 0)
	t := time.Now().Add(time.Hour * 72 * -1)
	err := clickhouse.CH.Select(context.Background(), &results, sql, StatusTxInfoUnprocessed, t.Format(timeLayout))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return results, nil
}
