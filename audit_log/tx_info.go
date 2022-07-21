package audit_log

import (
	"time"
)

type TxInfo struct {
	TxTime   time.Time `db:"time"`
	Context  string    `db:"context"`
	UserUUID string    `db:"user_uuid"`
	GTID     string    `db:"gtid"`
}

var (
	defaultLoc *time.Location
)

func NewTxInfo(ctx, userUUID, GTID string) *TxInfo {
	return &TxInfo{
		TxTime:   time.Now().In(defaultLoc),
		Context:  ctx,
		UserUUID: userUUID,
		GTID:     GTID,
	}
}

func init() {
	var err error
	defaultLoc, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic("timezone 'Asia/Shanghai' not found")
	}
}
