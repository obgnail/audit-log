package main

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/audit-log/compose/mysql"
	"github.com/obgnail/audit-log/compose/river"
	"github.com/obgnail/audit-log/context"
	"github.com/obgnail/audit-log/utils/uuid"
	r "github.com/obgnail/mysql-river/river"
	"gopkg.in/gorp.v1"
	"time"
)

func main() {
	go runRiver()
	time.Sleep(time.Second * 3)

	//createTable()
	insertUser()
	//dropTable()
	forever := make(chan struct{})
	<-forever
}

func runRiver() {
	compose.Init("../config/config.toml")

	go river.AuditLogRiver.ConsumeLog(func(event *common.BinlogEvent) error {
		fmt.Printf("在%d时刻,对%s.%s表进行了%s操作,其gtid为%s.操作的值为%s\n",
			event.Time, event.Db, event.Table, event.Action, event.GTID, event.Data)
		return nil
	})
	go river.AuditLogRiver.ConsumeTx(func(info *common.TxInfo) error {
		fmt.Printf("在%d时刻,提交的GTID为:%s,携带的上下文为:%s\n", info.Time, info.GTID, info.Context)
		return nil
	})

	err := river.Run(r.FromDB)
	checkErr(err)
}

func createTable() {
	err := mysql.DBMTransact("", func(tx *gorp.Transaction) error {
		sql := `
			CREATE TABLE IF NOT EXISTS user (
			  uuid varchar(8) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
			  name varchar(64) NOT NULL DEFAULT '' COMMENT '姓名',
			  email varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT '' COMMENT '邮箱',
			  status tinyint(4) NOT NULL DEFAULT '1' COMMENT '1.正常 2.删除',
			  PRIMARY KEY (uuid)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
		if _, err := tx.Exec(sql); err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	checkErr(err)
}

func dropTable() {
	err := mysql.DBMTransact("", func(tx *gorp.Transaction) error {
		sql := `DROP TABLE IF EXISTS user;`
		if _, err := tx.Exec(sql); err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	checkErr(err)
}

func insertUser() {
	myType := 1
	myContext := context.New(myType, "typeParam1", "typeParam2")
	err := mysql.DBMTransact(myContext.String(), func(tx *gorp.Transaction) error {
		_uuid := uuid.UUID()
		user := &User{_uuid, _uuid + "Name", _uuid + "@gmail.com", 0}
		return errors.Trace(AddUser(tx, user))
	})
	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
