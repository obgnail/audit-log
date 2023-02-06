package mysql

import (
	"database/sql"
	"fmt"
	"github.com/juju/errors"
	_ "github.com/obgnail/audit-log/compose/mysql/go-mysql-driver"
	"github.com/obgnail/audit-log/config"
	"gopkg.in/gorp.v1"
	"time"
)

func BuildDBMs(config *config.MySqlConfig) (dbms []*gorp.DbMap, err error) {
	for _, schema := range config.Schemas {
		dbm, err := buildDBM(config, schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dbms = append(dbms, dbm)
	}
	return dbms, nil
}

func buildDBM(config *config.MySqlConfig, schema string) (*gorp.DbMap, error) {
	connStr := fmt.Sprintf(
		`%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Asia%%2FShanghai&charset=utf8mb4`,
		config.User, config.Password, config.Host, config.Port, schema,
	)
	db, err := sql.Open(config.Driver, connStr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	db.SetMaxIdleConns(config.DbMaxIdle)
	db.SetMaxOpenConns(config.DbMaxOpen)
	connMaxLifetime := config.DbConnMaxLifeTime
	if connMaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)
	}
	dbm := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{}}
	return dbm, nil
}

var DBMList []*gorp.DbMap
var DBM *gorp.DbMap

func InitDBM() (err error) {
	DBMList, err = BuildDBMs(config.MySQL)
	if err != nil {
		return errors.Trace(err)
	}
	if len(DBMList) > 0 {
		DBM = DBMList[0]
	}
	return nil
}
