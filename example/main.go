package main

import (
	"fmt"
	"github.com/obgnail/audit-log/audit_log"
	"github.com/obgnail/audit-log/compose/canal"
	"github.com/obgnail/audit-log/compose/mysql"
	"github.com/obgnail/audit-log/context"
	"github.com/obgnail/audit-log/model"
	"gopkg.in/gorp.v1"
	"time"
)

func getUserUUID() string {
	return "userUUID"
}

func getTeamUUID() string {
	return "teamUUID"
}

func main() {
	go canal.StartCanal()

	time.Sleep(time.Second * 2)

	users, err := model.ListUsers(mysql.DBM)
	checkErr(err)
	fmt.Println(users)

	teamUUID := getTeamUUID()
	userUUID := getUserUUID()
	teamContent := context.TeamContext(teamUUID)
	err = mysql.DBMTransact(teamContent.String(), userUUID, func(tx *gorp.Transaction) error {
		if err := model.UpdateUserModifyTime(tx, "4owY4sL1"); err != nil {
			return err
		}
		return nil
	})
	checkErr(err)

	txInfo := audit_log.GetTxInfo()
	fmt.Println("--+++--", txInfo.GTID)

	time.Sleep(time.Second * 2)
	binlog := audit_log.GetBinlog()
	fmt.Println("--+++--", binlog)

	forever := make(chan struct{})
	<-forever
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
