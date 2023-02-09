module github.com/obgnail/audit-log

go 1.18

replace github.com/obgnail/audit-log/go-mysql-driver => ./compose/mysql/go-mysql-driver

require (
	github.com/BurntSushi/toml v1.2.1
	github.com/ClickHouse/clickhouse-go/v2 v2.0.12
	github.com/Shopify/sarama v1.37.0
	github.com/juju/errors v0.0.0-20220203013757-bd733f3c86b9
	github.com/obgnail/audit-log/go-mysql-driver v0.0.0-00010101000000-000000000000
	github.com/obgnail/mysql-river v0.0.0-20230208182038-dc88b534228c
	github.com/satori/go.uuid v1.2.0
	gopkg.in/gorp.v1 v1.7.2
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/etcd-io/bbolt v1.3.3 // indirect
	github.com/go-mysql-org/go-mysql v1.7.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.15.11 // indirect
	github.com/lib/pq v1.8.1-0.20200908161135-083382b7e6fc // indirect
	github.com/mattn/go-sqlite3 v1.14.12 // indirect
	github.com/paulmach/orb v0.4.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63 // indirect
	github.com/pingcap/log v0.0.0-20210625125904-98ed8e2eb1c7 // indirect
	github.com/pingcap/tidb/parser v0.0.0-20221126021158-6b02a5d8ba7d // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/ziutek/mymysql v1.5.4 // indirect
	go.opentelemetry.io/otel v1.4.1 // indirect
	go.opentelemetry.io/otel/trace v1.4.1 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.18.1 // indirect
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa // indirect
	golang.org/x/net v0.0.0-20220927171203-f486391704dc // indirect
	golang.org/x/sys v0.4.0 // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
)
