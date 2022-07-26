package canal

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/obgnail/audit-log/audit_log"
	"github.com/pingcap/errors"
	"log"
	"runtime/debug"
	"strings"
)

type BinlogHandler struct {
	databases map[string]struct{}
	tables    map[string]struct{}

	curGTID string

	*canal.DummyEventHandler
}

func NewBinlogHandler(databases, tables []string) *BinlogHandler {
	ds := make(map[string]struct{}, len(databases))
	ts := make(map[string]struct{}, len(tables))
	for _, d := range databases {
		ds[strings.ToLower(d)] = struct{}{}
	}
	for _, t := range tables {
		ts[strings.ToLower(t)] = struct{}{}
	}
	return &BinlogHandler{
		databases:         ds,
		tables:            ts,
		DummyEventHandler: new(canal.DummyEventHandler),
	}
}

func (h *BinlogHandler) String() string {
	return "binlog log"
}

func (h *BinlogHandler) OnGTID(gtid mysql.GTIDSet) error {
	h.curGTID = gtid.String()
	return nil
}

func (h *BinlogHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	if len(h.databases) != 0 {
		if _, ok := h.databases[e.Table.Schema]; !ok {
			return nil
		}
	}
	if len(h.tables) != 0 {
		if _, ok := h.tables[e.Table.Name]; !ok {
			return nil
		}
	}

	db := e.Table.Schema
	table := e.Table.Name
	var action int
	before := make(map[string]interface{})
	after := make(map[string]interface{})

	switch e.Action {
	case canal.UpdateAction:
		action = audit_log.EventActionUpdate
		before = buildFields(e.Table.Columns, e.Rows[0])
		after = buildFields(e.Table.Columns, e.Rows[1])
	case canal.InsertAction:
		action = audit_log.EventActionInsert
		after = buildFields(e.Table.Columns, e.Rows[0])
	case canal.DeleteAction:
		action = audit_log.EventActionDelete
		before = buildFields(e.Table.Columns, e.Rows[0])
	}

	data, err := Marshal(before, after)
	if err != nil {
		log.Printf("[ERR] %s", err)
	}

	audit_log.SaveBinlog(db, table, h.curGTID, action, e.Header.Timestamp, data)
	return nil
}

func buildFields(columns []schema.TableColumn, fields []interface{}) map[string]interface{} {
	res := make(map[string]interface{}, len(fields))
	for idx, field := range fields {
		key := columns[idx].Name
		res[key] = field
	}
	return res
}

type FormatData struct {
	Before map[string]interface{} `json:"before"` // 变更前数据, insert 类型的 before 为空
	After  map[string]interface{} `json:"after"`  // 变更后数据, delete 类型的 after 为空
}

func Marshal(before, after map[string]interface{}) ([]byte, error) {
	data := &FormatData{
		Before: before,
		After:  after,
	}
	res, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return res, nil
}
