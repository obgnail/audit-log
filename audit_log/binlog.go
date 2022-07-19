package audit_log

import "database/sql"

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

func NewBinlogEvent(db, table, gtid string, action int, eventTime uint32, event sql.RawBytes) *BinlogEvent {
	return &BinlogEvent{
		Db:          db,
		EventTable:  table,
		EventAction: action,
		GTID:        gtid,
		EventTime:   eventTime,
		Event:       event,
	}
}
