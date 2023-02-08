package syncer

import (
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/broker"
	"github.com/obgnail/audit-log/compose/clickhouse"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/audit-log/config"
	"github.com/obgnail/mysql-river/handler/kafka"
	"github.com/obgnail/mysql-river/river"
	"time"
)

type BinlogSynchronizer struct {
	*river.River
	*broker.BinlogKafkaBroker

	syncChan chan *common.BinlogEvent
	errChan  chan error
}

func NewBinlogSyncer(river *river.River, broker *broker.BinlogKafkaBroker) *BinlogSynchronizer {
	s := &BinlogSynchronizer{
		River:             river,
		BinlogKafkaBroker: broker,
		syncChan:          make(chan *common.BinlogEvent, 512),
		errChan:           make(chan error, 10),
	}
	return s
}

func (s *BinlogSynchronizer) handleError() {
	for err := range s.errChan {
		river.Logger.Error(err.Error())
	}
}

func (s *BinlogSynchronizer) batchSend() {
	var events []clickhouse.BinlogEvent

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		needSend := false
		select {
		case <-ticker.C:
			needSend = true
		case event := <-s.syncChan:
			binlogEvent := clickhouse.ConvertBinlog(event)
			events = append(events, binlogEvent)
			needSend = len(events) >= 512
		}

		if needSend && len(events) != 0 {
			if err := clickhouse.InsertBinlogEvents(events); err != nil {
				s.errChan <- errors.Trace(err)
			}
			events = events[0:0]
		}
	}
}

func (s *BinlogSynchronizer) Sync(from river.From) error {
	go s.handleError()
	go s.batchSend()
	go func() {
		if err := s.River.SetHandler(s.Broker).Sync(from); err != nil {
			s.errChan <- errors.Trace(err)
		}
	}()

	err := s.BinlogKafkaBroker.Consume(func(event *common.BinlogEvent) error {
		s.syncChan <- event
		return nil
	})
	return errors.Trace(err)
}

func newRiver() *river.River {
	MySQL := config.MySQL
	PositionSaver := config.PositionSaver
	HealthChecker := config.HealthChecker
	cfg := &river.Config{
		MySQLConfig: &river.MySQLConfig{
			Host:     MySQL.Host,
			Port:     MySQL.Port,
			User:     MySQL.User,
			Password: MySQL.Password,
		},
		PosAutoSaverConfig: &river.PosAutoSaverConfig{
			SaveDir:      PositionSaver.SaveDir,
			SaveInterval: time.Duration(PositionSaver.SaveInterval) * time.Second,
		},
		HealthCheckerConfig: &river.HealthCheckerConfig{
			CheckPosThreshold: HealthChecker.CheckPosThreshold,
			CheckInterval:     time.Duration(HealthChecker.CheckInterval) * time.Second,
		},
	}
	return river.New(cfg)
}

func newBroker() (*broker.BinlogKafkaBroker, error) {
	KafkaCfg := config.Kafka
	cfg := &broker.BinlogBrokerConfig{
		KafkaConfig: &kafka.Config{
			Addrs:           KafkaCfg.Addrs,
			Topic:           KafkaCfg.BinlogTopic,
			OffsetStoreDir:  KafkaCfg.OffsetStoreDir,
			Offset:          KafkaCfg.Offset,
			UseOldestOffset: KafkaCfg.UseOldestOffset,
		},
		Tables: config.AuditLog.HandleTables,
	}
	return broker.New(cfg)
}

var (
	BinlogSyncer *BinlogSynchronizer
)

func InitBinlogSyncer() (err error) {
	_river := newRiver()
	_broker, err := newBroker()
	if err != nil {
		return errors.Trace(err)
	}
	BinlogSyncer = NewBinlogSyncer(_river, _broker)
	return nil
}
