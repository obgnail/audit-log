package mysql

import (
	"bytes"
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/audit-log/compose/river"
	uuid "github.com/satori/go.uuid"
	"gopkg.in/gorp.v1"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	sqlDefaultSep = " ,"
)

func DBMTransact(ctx, userUUID string, txFunc func(*gorp.Transaction) error) (err error) {
	tx, err := DBM.Begin()
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%s", p)
			}
		}
		if err != nil {
			tx.Rollback()
			return
		}
		txiField := reflectTxi(tx)

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return
		}

		GTIDField := txiField.FieldByName("GTID")
		GTIDValue := GTIDField.String()
		if checkGTID(GTIDValue) {
			t := common.NewTxInfo(ctx, userUUID, GTIDValue)
			if err := river.AuditLogRiver.PushTx(t); err != nil {
				log.Println("[Error] push tx err:\n", errors.ErrorStack(err))
			}
			fmt.Printf("GTIDField: %s, GTIDValue: %s\n", GTIDField, GTIDValue)
		}
	}()
	return txFunc(tx)
}

func reflectTxi(tx *gorp.Transaction) reflect.Value {
	obj := reflect.ValueOf(tx)
	elem := obj.Elem()
	txField := elem.FieldByName("tx")
	txElem := txField.Elem()
	txiField := txElem.FieldByName("txi").Elem().Elem()
	return txiField
}

func checkGTID(GTID string) bool {
	str := strings.TrimSpace(GTID)
	sep := strings.Split(str, ":")
	if len(sep) < 2 {
		return false
	}

	var err error
	if _, err = uuid.FromString(sep[0]); err != nil {
		return false
	}

	// Handle interval
	for i := 1; i < len(sep); i++ {
		if err := parseInterval(sep[i]); err != nil {
			return false
		}
	}
	return true
}

// Interval is [start, stop), but the GTID string's format is [n] or [n1-n2], closed interval
func parseInterval(str string) (err error) {
	p := strings.Split(str, "-")
	switch len(p) {
	case 1:
		_, err = strconv.ParseInt(p[0], 10, 64)
	case 2:
		_, err = strconv.ParseInt(p[0], 10, 64)
		_, err = strconv.ParseInt(p[1], 10, 64)
	default:
		err = fmt.Errorf("invalid interval format, must n[-n]")
	}

	if err != nil {
		return
	}
	return
}

func BuildSqlArgs(args ...interface{}) ([]interface{}, error) {
	newArgs := make([]interface{}, 0)
	addEleFun := func(ele interface{}) {
		newArgs = append(newArgs, ele)
		return
	}
	for _, arg := range args {
		switch v := arg.(type) {
		case string, int, int32, int64, bool, *string, *int, *int32, *int64, time.Time:
			addEleFun(v)
		case []string:
			for _, e := range v {
				addEleFun(e)
			}
		case []int:
			for _, e := range v {
				addEleFun(e)
			}
		case []int32:
			for _, e := range v {
				addEleFun(e)
			}
		case []int64:
			for _, e := range v {
				addEleFun(e)
			}
		case []*string:
			for _, e := range v {
				addEleFun(e)
			}
		case nil:
			addEleFun(v)
		default:
			return nil, fmt.Errorf("error arg: %v", args)
		}
	}
	return newArgs, nil
}

// SqlPlaceholders 创建 sql in 查询条件
func SqlPlaceholders(count int) string {
	return appendDuplicateString("?", sqlDefaultSep, count)
}

func appendDuplicateString(character, separator string, count int) string {
	if count <= 0 {
		return ""
	}
	var b bytes.Buffer
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteString(separator)
		}
		b.WriteString(character)
	}
	return b.String()
}
