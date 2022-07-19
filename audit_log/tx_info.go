package audit_log

import (
	"database/sql"
	utils "github.com/obgnail/audit-log/compose/Queue"
	"time"
)

var (
	binlogQueue *utils.Queue
	txInfoQueue *utils.Queue
)

var (
	defaultLoc *time.Location
)

func init() {
	var err error
	defaultLoc, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic("timezone 'Asia/Shanghai' not found")
	}
}

type TxInfo struct {
	Time     time.Time `db:"time"`
	Context  string    `db:"context"`
	UserUUID string    `db:"user_uuid"`
	GTID     string    `db:"gtid"`
}

func NewTxInfo(ctx, userUUID, Gtid string) *TxInfo {
	return &TxInfo{
		Time:     time.Now().In(defaultLoc),
		Context:  ctx,
		UserUUID: userUUID,
		GTID:     Gtid,
	}
}

func SaveTxInfo(ctx, userUUID, Gtid string) {
	txInfo := NewTxInfo(ctx, userUUID, Gtid)
	txInfoQueue.Push(txInfo)
}

func GetTxInfo() *TxInfo {
	res := txInfoQueue.Pop()
	if res == nil {
		return nil
	}
	return res.(*TxInfo)
}

func SaveBinlog(db, table, gtid string, action int, eventTime uint32, event sql.RawBytes) {
	e := NewBinlogEvent(db, table, gtid, action, eventTime, event)
	binlogQueue.Push(e)
}

func GetBinlog() *BinlogEvent {
	res := binlogQueue.Pop()
	if res == nil {
		return nil
	}
	return res.(*BinlogEvent)
}

func init() {
	binlogQueue = utils.NewQueue()
	txInfoQueue = utils.NewQueue()
}
