package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"os"
)

type MainConfig struct {
	Mysql         *MySqlConfig           `toml:"mysql"`
	PositionSaver *PosAutoSaverConfig    `toml:"position_saver"`
	HealthChecker *HealthCheckerConfig   `toml:"health_checker"`
	AuditLog      *AuditLogHandlerConfig `toml:"river"`
	Kafka         *KafkaConfig           `toml:"kafka"`
	ClickHouse    *ClickHouseConfig      `toml:"clickhouse"`
}

type MySqlConfig struct {
	Driver            string   `toml:"driver"`
	Host              string   `toml:"host"`
	Port              int64    `toml:"port"`
	User              string   `toml:"user"`
	Password          string   `toml:"password"`
	Schemas           []string `toml:"schemas"`
	DbMaxIdle         int      `toml:"db_max_idle"`
	DbMaxOpen         int      `toml:"db_max_open"`
	DbConnMaxLifeTime int      `toml:"db_conn_max_life_time"`
}

type PosAutoSaverConfig struct {
	SaveDir      string `toml:"save_dir"`
	SaveInterval int    `toml:"save_interval"`
}

type HealthCheckerConfig struct {
	CheckInterval     int `toml:"check_interval"`
	CheckPosThreshold int `toml:"check_pos_threshold"`
}

type AuditLogHandlerConfig struct {
	HandleTables []string `toml:"handle_tables"`
}

type KafkaConfig struct {
	Addrs       []string `toml:"addrs"`
	BinlogTopic string   `toml:"binlog_topic"`
	TxInfoTopic string   `toml:"tx_info_topic"`
}

type ClickHouseConfig struct {
	Host string `toml:"host"`
	port int64  `toml:"port"`
}

var (
	Main          *MainConfig
	MySQL         *MySqlConfig
	PositionSaver *PosAutoSaverConfig
	HealthChecker *HealthCheckerConfig
	AuditLog      *AuditLogHandlerConfig
	Kafka         *KafkaConfig
	ClickHouse    *ClickHouseConfig
)

func InitConfig() error {
	var cfg MainConfig
	f, err := os.Open("./config/config.toml")
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()
	_, err = toml.NewDecoder(f).Decode(&cfg)
	if err != nil {
		return errors.Trace(err)
	}

	Main = &cfg
	MySQL = Main.Mysql
	PositionSaver = Main.PositionSaver
	HealthChecker = Main.HealthChecker
	AuditLog = Main.AuditLog
	Kafka = Main.Kafka
	ClickHouse = Main.ClickHouse
	return nil
}
