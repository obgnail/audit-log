module github.com/obgnail/audit-log

go 1.16

replace github.com/obgnail/audit-log/go-mysql-driver => ./compose/mysql/go-mysql-driver

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.2.0
	github.com/Shopify/sarama v1.34.1
	github.com/etcd-io/bbolt v1.3.3 // indirect
	github.com/go-mysql-org/go-mysql v1.6.0
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/juju/errors v0.0.0-20220203013757-bd733f3c86b9
	github.com/juju/testing v1.0.1 // indirect
	github.com/lib/pq v1.8.1-0.20200908161135-083382b7e6fc // indirect
	github.com/mattn/go-sqlite3 v1.14.12 // indirect
	github.com/obgnail/audit-log/go-mysql-driver v0.0.0-00010101000000-000000000000
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/satori/go.uuid v1.2.0
	github.com/ziutek/mymysql v1.5.4 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
	gopkg.in/gorp.v1 v1.7.2
)
