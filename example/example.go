package main

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/audit_log"
	"github.com/obgnail/audit-log/context"
	"github.com/obgnail/audit-log/mysql"
	"github.com/obgnail/audit-log/mysql/utils/uuid"
	"github.com/obgnail/audit-log/types"
	"gopkg.in/gorp.v1"
	"time"
)

func main() {
	audit_log.Init("./config/config.toml")

	audit_log.Run(audit_log.FunctionHandler(func(auditLog *types.AuditLog) error {
		fmt.Printf("get audit log: %+v\n", *auditLog)
		return nil
	}))

	time.Sleep(time.Second * 3)

	//createTable()
	insertUser()
	//dropTable()

	forever := make(chan struct{})
	<-forever
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
	// 每个具体业务对应一个Context。以【插入用户】为例
	// Context 中包含业务代码兴趣的环境信息
	insertUserContext := 1
	myContext := context.New(insertUserContext, "ContextParam1", "ContextParam2")

	// 执行MySQL事务,携带上面的Context
	err := mysql.DBMTransact(myContext.String(), func(tx *gorp.Transaction) error {
		_uuid := uuid.UUID()
		user := &User{_uuid, _uuid + "Name", _uuid + "@gmail.com", 0}
		sql := "INSERT INTO `user` (uuid, name, email, status) VALUES (?, ?, ?, ?);"
		args, _ := mysql.BuildSqlArgs(user.UUID, user.Name, user.Email, user.Status)
		if _, err := tx.Exec(sql, args...); err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
