package compose

import (
	"github.com/obgnail/audit-log/compose/clickhouse"
	"github.com/obgnail/audit-log/compose/logger"
	"github.com/obgnail/audit-log/compose/mysql"
	"github.com/obgnail/audit-log/compose/syncer"
	"github.com/obgnail/audit-log/config"
)

func Init(path string) {
	checkErr(config.InitConfig(path))
	checkErr(logger.InitLogger())
	checkErr(syncer.InitBinlogSyncer())
	checkErr(syncer.InitTxInfoSyncer())
	checkErr(mysql.InitDBM())
	checkErr(clickhouse.InitClickHouse())
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
