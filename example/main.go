package main

import (
	"fmt"
	_ "github.com/obgnail/audit-log/compose"
	"github.com/obgnail/audit-log/compose/mysql"
	"github.com/obgnail/audit-log/context"
	"github.com/obgnail/audit-log/models"
	"gopkg.in/gorp.v1"
)

func getUserUUID() string {
	return "userUUID1"
}

func getTeamUUID() string {
	return "teamUUID1"
}

func main() {
	users, err := models.ListUsers(mysql.DBM)
	checkErr(err)
	fmt.Println(users)

	teamUUID := getTeamUUID()
	userUUID := getUserUUID()
	teamContent := context.TeamContext(teamUUID)
	err = mysql.DBMTransact(teamContent.String(), userUUID, func(tx *gorp.Transaction) error {
		if err := models.UpdateUserModifyTime(tx, "4owY4sL1"); err != nil {
			return err
		}
		return nil
	})
	checkErr(err)

	//txInfo := audit_log.GetTxInfo()
	//fmt.Println("--+++--", txInfo.GTID)
	//
	//time.Sleep(time.Second * 2)
	//binlog := audit_log.GetBinlog()
	//fmt.Println("--+++--", binlog)

	forever := make(chan struct{})
	<-forever
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
