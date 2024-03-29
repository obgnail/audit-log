package syncer

import (
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/broker"
	"github.com/obgnail/audit-log/config"
	"github.com/obgnail/audit-log/logger"
	"github.com/obgnail/audit-log/types"
	"github.com/obgnail/mysql-river/handler/kafka"
	"github.com/obgnail/mysql-river/river"
	"time"
)

const (
	defaultSyncChanSize      = 1024
	defaultBulkSize          = 512
	defaultBatchSendInterval = 1 * time.Second
)

// BinlogSynchronizer 将river中的数据通过broker流向clickhouse
type BinlogSynchronizer struct {
	river  *river.River
	broker *broker.BinlogKafkaBroker

	syncChan chan *types.BinlogEvent
}

func NewBinlogSyncer(river *river.River, broker *broker.BinlogKafkaBroker) *BinlogSynchronizer {
	s := &BinlogSynchronizer{
		river:    river,
		broker:   broker,
		syncChan: make(chan *types.BinlogEvent, defaultSyncChanSize),
	}
	return s
}

func (s *BinlogSynchronizer) batchSend2Clickhouse() {
	var bulk []types.ChBinlogEvent

	ticker := time.NewTicker(defaultBatchSendInterval)
	defer ticker.Stop()

	for {
		needSend := false
		select {
		case <-ticker.C:
			needSend = true
		case event := <-s.syncChan:
			chEvent := event.ChEvent()
			bulk = append(bulk, chEvent)
			needSend = len(bulk) >= defaultBulkSize
		}

		if needSend && len(bulk) != 0 {
			err := types.InsertBinlogEvents(bulk)
			if err != nil {
				logger.ErrorDetails(errors.Trace(err))
				logger.Error("error events:")
				for _, e := range bulk {
					logger.Error("%+v", e)
				}
			}
			bulk = bulk[0:0]
		}
	}
}

func (s *BinlogSynchronizer) Sync() {
	go s.batchSend2Clickhouse()

	go func() {
		err := s.broker.Consume(func(event *types.BinlogEvent) error {
			s.syncChan <- event
			return nil
		})
		if err != nil {
			logger.ErrorDetails(errors.Trace(err))
		}
	}()
	go func() {
		err := s.broker.Pipe(s.river, river.FromFile)
		if err != nil {
			logger.ErrorDetails(errors.Trace(err))
		}
	}()
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
	b, err := broker.New(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return b, nil
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
