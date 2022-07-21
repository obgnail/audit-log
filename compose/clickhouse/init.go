package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/obgnail/audit-log/config"
	"github.com/pingcap/errors"
	"time"
)

var CH clickhouse.Conn

var auditLogTables = map[string]string{
	"obj_mapping": `
CREATE TABLE %s.obj_mapping
(
    key String,
    name Nullable(String),
    field_values Nullable(String)
)ENGINE=EmbeddedRocksDB PRIMARY KEY(key);
`,
	"binlog_event": `
CREATE TABLE %s.binlog_event
(
    event_db String,
    event_table String,
    event_action Int32,
    gtid String,
    event String,
    event_time DateTime DEFAULT now()
)ENGINE=MergeTree()
PARTITION BY toYYYYMMDD(event_time) ORDER BY gtid
TTL event_time + INTERVAL 30 DAY;
`,
	"tx_info": `
CREATE TABLE %s.tx_info
(
    gtid String,
    context String,
    user_uuid String,
    action_context Int32,
    ip Nullable(IPv4),
    tx_time DateTime64(3, 'Asia/Shanghai'),
    status UInt8
)ENGINE=ReplacingMergeTree()
PARTITION BY toYYYYMM(tx_time) ORDER BY gtid
TTL toDateTime(tx_time) + INTERVAL 60 DAY;
`,
	"audit_log": `
CREATE TABLE %s.audit_log
(
    gtid String,
    context String,
    operator_uuid String,
    action Int32,
    action_context Int32,
    object_type String,
    data String,
    ip Nullable(IPv4),
    tx_time DateTime64(3, 'Asia/Shanghai')
)ENGINE=MergeTree()
PARTITION BY toYYYYMM(tx_time) ORDER BY (toDateTime(tx_time), context, action, operator_uuid);
`,
}

func InitClickHouse() error {
	addrs := config.StringSlice("clickhouse_addrs")
	chDatabase := config.String("clickhouse_database", "audit_log")
	user := config.String("clickhouse_user", "default")
	password := config.String("clickhouse_password", "")
	maxIdle := config.Int("clickhouse_max_idle", 5)
	maxOpen := config.Int("clickhouse_max_open", 10)
	debugCH := config.Bool("clickhouse_debug", false)
	opt := &clickhouse.Options{
		Addr: addrs,
		Auth: clickhouse.Auth{
			//Database: chDatabase,
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns:    maxOpen,
		MaxIdleConns:    maxIdle,
		ConnMaxLifetime: time.Hour,
		Debug:           debugCH,
	}
	conn, err := clickhouse.Open(opt)
	if err != nil {
		return errors.Trace(err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return errors.Trace(err)
	}
	err = initDatabase(conn, chDatabase)
	if err != nil {
		return errors.Trace(err)
	}
	opt.Auth.Database = chDatabase
	conn, err = clickhouse.Open(opt)
	if err != nil {
		return errors.Trace(err)
	}
	CH = conn
	return nil
}

func initDatabase(conn clickhouse.Conn, db string) error {
	existDbSql := "SELECT name FROM system.databases;"
	rows, err := conn.Query(context.Background(), existDbSql)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	foundDb := false
	for rows.Next() {
		var dbName string
		err = rows.Scan(&dbName)
		if err != nil {
			return errors.Trace(err)
		}
		if dbName == db {
			foundDb = true
			break
		}
	}
	if !foundDb {
		createDbSql := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS "%s";`, db)
		err = conn.Exec(context.Background(), createDbSql)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return initTable(conn, db)
}

func initTable(conn clickhouse.Conn, db string) error {
	existTableSql := "SELECT name FROM system.tables WHERE database=$1;"
	tableNameRows, err := conn.Query(context.Background(), existTableSql, db)
	if err != nil {
		return errors.Trace(err)
	}
	defer tableNameRows.Close()

	mapTable := make(map[string]struct{}, 4)
	for tableNameRows.Next() {
		var tableName string
		err = tableNameRows.Scan(&tableName)
		if err != nil {
			return errors.Trace(err)
		}
		if tableName != "" {
			mapTable[tableName] = struct{}{}
		}
	}
	createTableSqls := make([]string, 0, 4)
	for table, _createTableSql := range auditLogTables {
		if _, found := mapTable[table]; !found {
			createTableSqls = append(createTableSqls, fmt.Sprintf(_createTableSql, db))
		}
	}
	for _, createTableSql := range createTableSqls {
		err = conn.Exec(context.Background(), createTableSql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
