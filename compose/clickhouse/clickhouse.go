package clickhouse

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/config"
	"log"
	"time"
)

var CH clickhouse.Conn

func InitClickHouse() (err error) {
	click := config.ClickHouse
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: click.Addrs,
		Auth: clickhouse.Auth{
			Database: click.DB,
			Username: click.User,
			Password: click.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     10 * time.Second,
		Debug:           click.Debug,
		MaxOpenConns:    15,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		return errors.Trace(err)
	}
	//ChContext = clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
	//	"max_block_size": 10,
	//}), clickhouse.WithProgress(func(p *clickhouse.Progress) {
	//	fmt.Println("progress: ", p)
	//}))
	for i := 0; i < 3; i++ {
		err = conn.Ping(context.Background())
		if err == nil {
			break
		}
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		log.Fatalln(err)
	}

	CH = conn
	return nil
}
