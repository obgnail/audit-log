package types

import (
	"database/sql"
	"encoding/json"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/river"
	"time"
)

type Action int

const (
	EventActionInsert Action = iota
	EventActionUpdate
	EventActionDelete
)

func (a Action) String() string {
	switch a {
	case EventActionInsert:
		return "insert"
	case EventActionUpdate:
		return "update"
	case EventActionDelete:
		return "delete"
	default:
		return "unknown"
	}
}

type BinlogEvent struct {
	Db     string       `json:"db"`
	Table  string       `json:"table"`
	Action Action       `json:"action"`
	GTID   string       `json:"gtid"`
	Time   int64        `json:"time"`
	Data   sql.RawBytes `json:"data"`
}

func (e *BinlogEvent) ChEvent() ChBinlogEvent {
	return ChBinlogEvent{
		Db:     e.Db,
		Table:  e.Table,
		Action: int32(e.Action),
		GTID:   e.GTID,
		Data:   string(e.Data),
	}
}

func (e *BinlogEvent) Marshal() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return []byte{}, errors.Trace(err)
	}
	return b, nil
}

func NewBinlogEvent(event *river.EventData) (*BinlogEvent, error) {
	data, err := marshal(event.Before, event.After)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var action Action
	switch event.EventType {
	case river.EventTypeInsert:
		action = EventActionInsert
	case river.EventTypeUpdate:
		action = EventActionUpdate
	case river.EventTypeDelete:
		action = EventActionDelete
	}

	b := &BinlogEvent{
		Db:     event.Db,
		Table:  event.Table,
		Action: action,
		GTID:   event.GTIDSet,
		Time:   int64(event.Timestamp),
		Data:   data,
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
	Time    int64  `db:"time" json:"time"`
	Context string `db:"context" json:"context"`
	GTID    string `db:"gtid" json:"gtid"`
}

func (t *TxInfo) ChTxInfo(status uint8) ChTxInfo {
	return ChTxInfo{
		Time:    time.Unix(t.Time, 0),
		Context: t.Context,
		GTID:    t.GTID,
		Status:  status,
	}
}

func (t *TxInfo) Marshal() ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return []byte{}, errors.Trace(err)
	}
	return b, nil
}

func NewTxInfo(ctx, Gtid string) *TxInfo {
	return &TxInfo{
		Time:    time.Now().In(defaultLoc).Unix(),
		Context: ctx,
		GTID:    Gtid,
	}
}

func init() {
	var err error
	defaultLoc, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic("timezone 'Asia/Shanghai' not found")
	}
}
