package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"os"
)

type MainConfig struct {
	Log           *LogConfig             `toml:"log"`
	Mysql         *MySqlConfig           `toml:"mysql"`
	PositionSaver *PosAutoSaverConfig    `toml:"position_saver"`
	HealthChecker *HealthCheckerConfig   `toml:"health_checker"`
	AuditLog      *AuditLogHandlerConfig `toml:"audit_log"`
	Kafka         *KafkaConfig           `toml:"kafka"`
	ClickHouse    *ClickHouseConfig      `toml:"clickhouse"`
}

type LogConfig struct {
	LogFile  string `toml:"file"`
	LogLevel string `toml:"level"`
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
	Addrs           []string `toml:"addrs"`
	BinlogTopic     string   `toml:"binlog_topic"`
	TxInfoTopic     string   `toml:"tx_info_topic"`
	OffsetStoreDir  string   `toml:"offset_store_dir"`
	Offset          *int64   `toml:"offsetStore"` // if it has no offset, set nil
	UseOldestOffset bool     `toml:"use_oldest_offset"`
}

type ClickHouseConfig struct {
	Addrs    []string `toml:"addrs"`
	User     string   `toml:"user"`
	Password string   `toml:"password"`
	DB       string   `toml:"db"`
	Debug    bool     `toml:"debug"`
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

func FindConfigPath(configPath string) string {
	// 向上找5层, 满足在一些单元测试中加载不了配置文件的问题
	for i := 0; i < 5; i++ {
		if _, err := os.Stat(configPath); err == nil {
			return configPath
		} else {
			configPath = "../" + configPath
		}
	}
	panic("not found config file in path")
}

func InitConfig(path string) error {
	path = FindConfigPath(path)
	var cfg MainConfig
	f, err := os.Open(path)
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
