package compose

import (
	"github.com/obgnail/audit-log/compose/broker"
	"github.com/obgnail/audit-log/compose/mysql"
	"github.com/obgnail/audit-log/compose/river"
	"github.com/obgnail/audit-log/config"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Init(path string) {
	checkErr(config.InitConfig(path))
	checkErr(broker.InitBroker())
	checkErr(river.InitRiver())
	checkErr(mysql.InitDBM())
}
