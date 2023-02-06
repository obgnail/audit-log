package main

import (
	"fmt"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/audit-log/compose/river"
	r "github.com/obgnail/mysql-river/river"
)

func main() {
	go river.AuditLogRiver.ConsumeLog(func(event *common.BinlogEvent) error {
		fmt.Println("--- event ---", event.GTID, event.EventAction, string(event.Event))
		return nil
	})

	err := river.Run(r.FromFile)
	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
