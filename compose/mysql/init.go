package mysql

import (
	"database/sql"
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/config"
	_ "github.com/obgnail/audit-log/go-mysql-driver"
	"gopkg.in/gorp.v1"
	"time"
)

var (
	DBM *gorp.DbMap
)

func InitMySQL() (err error) {
	dbm, err := buildDBM()
	if err != nil {
		return errors.Trace(err)
	}
	DBM = dbm
	return nil
}

func buildDBM() (*gorp.DbMap, error) {
	dbDriver := config.String("db_driver", "mysql")
	connStr := fmt.Sprintf(
		`%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Asia%%2FShanghai&charset=utf8mb4`,
		config.String("mysql_user", "root"),
		config.String("mysql_password", "root"),
		config.String("mysql_host", "127.0.0.1"),
		config.Int("mysql_port", 3306),
		config.String("schema", "mysql"),
	)
	db, err := sql.Open(dbDriver, connStr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	db.SetMaxIdleConns(config.Int("db_max_idle", 10))
	db.SetMaxOpenConns(config.Int("db_max_open", 1024))
	connMaxLifetime := config.Int("db_conn_max_life_time", 1024)
	if connMaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)
	}
	dbm := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{}}
	return dbm, nil
}
