package common

import (
	"database/sql"
	"encoding/json"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/river"
	"time"
)

const (
	EventActionInsert = iota
	EventActionUpdate
	EventActionDelete
)

type BinlogEvent struct {
	Db          string       `json:"db"`
	EventTable  string       `json:"event_table"`
	EventAction int          `json:"event_action"`
	GTID        string       `json:"gtid"`
	EventTime   uint32       `json:"event_time"`
	Event       sql.RawBytes `json:"event"`
}

func (e *BinlogEvent) Marshal() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return []byte{}, nil
	}
	return b, nil
}

func NewBinlogEvent(event *river.EventData) (*BinlogEvent, error) {
	data, err := marshal(event.Before, event.After)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var action int
	switch event.EventType {
	case river.EventTypeInsert:
		action = EventActionInsert
	case river.EventTypeUpdate:
		action = EventActionUpdate
	case river.EventTypeDelete:
		action = EventActionDelete
	}

	b := &BinlogEvent{
		Db:          event.Db,
		EventTable:  event.Table,
		EventAction: action,
		GTID:        event.GTIDSet,
		EventTime:   event.Timestamp,
		Event:       data,
	}
	return b, nil
}

type FormatData struct {
	Before map[string]interface{} `json:"before"` // 变更前数据, insert 类型的 before 为空
	After  map[string]interface{} `json:"after"`  // 变更后数据, delete 类型的 after 为空
}

func marshal(before, after map[string]interface{}) ([]byte, error) {
	data := &FormatData{Before: before, After: after}
	res, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return res, nil
}

var (
	defaultLoc *time.Location
)

type TxInfo struct {
	Time     int64  `db:"time" json:"time"`
	Context  string `db:"context" json:"context"`
	UserUUID string `db:"user_uuid" json:"user_uuid"`
	GTID     string `db:"gtid" json:"gtid"`
}

func (t *TxInfo) Marshal() ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return []byte{}, nil
	}
	return b, nil
}

func NewTxInfo(ctx, userUUID, Gtid string) *TxInfo {
	return &TxInfo{
		Time:     time.Now().In(defaultLoc).Unix(),
		Context:  ctx,
		UserUUID: userUUID,
		GTID:     Gtid,
	}
}

func init() {
	var err error
	defaultLoc, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic("timezone 'Asia/Shanghai' not found")
	}
}
