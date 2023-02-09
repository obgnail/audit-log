package broker

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/audit-log/compose/logger"
	"github.com/obgnail/mysql-river/handler/kafka"
	"github.com/obgnail/mysql-river/river"
	"strings"
)

type BinlogBrokerConfig struct {
	KafkaConfig *kafka.Config
	Tables      []string
}

type BinlogKafkaBroker struct {
	include map[string]map[string]struct{} // map[db]map[table]struct{}
	*kafka.Broker
}

func New(cfg *BinlogBrokerConfig) (*BinlogKafkaBroker, error) {
	h := new(BinlogKafkaBroker)
	mapDb2Table := make(map[string]map[string]struct{})
	for _, ele := range cfg.Tables {
		list := strings.Split(ele, ".")
		db, table := list[0], list[1]
		if _, ok := mapDb2Table[db]; !ok {
			mapDb2Table[db] = make(map[string]struct{})
		}
		mapDb2Table[db][table] = struct{}{}
	}
	h.include = mapDb2Table
	var err error
	h.Broker, err = kafka.New(cfg.KafkaConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	h.Broker.SetEventMarshaller(h.marshaller)
	return h, nil
}

func (b *BinlogKafkaBroker) String() string {
	return "binlog kafka broker"
}

func (b *BinlogKafkaBroker) check(db, table string) bool {
	if _, ok := b.include[db]; !ok {
		return false
	}
	if _, ok := b.include[db][table]; !ok {
		return false
	}
	return true
}

func (b *BinlogKafkaBroker) marshaller(event *river.EventData) ([]byte, error) {
	switch event.EventType {
	case river.EventTypeInsert, river.EventTypeUpdate, river.EventTypeDelete:
		if b.check(event.Db, event.Table) {
			binlog, err := common.NewBinlogEvent(event)
			if err != nil {
				logger.ErrorDetails(errors.Trace(err))
				return nil, nil
			}
			result, err := binlog.Marshal()
			if err != nil {
				logger.ErrorDetails(errors.Trace(err))
				return nil, nil
			}
			return result, nil
		}
	}
	return nil, nil
}

func (b *BinlogKafkaBroker) OnAlert(msg *river.StatusMsg) error {
	logger.Warn("binlog broker on alert: %+v", *msg)
	return nil
}

func (b *BinlogKafkaBroker) OnClose(r *river.River) {
	logger.ErrorDetails(r.Error)
	return
}

// Pipe 将river中的数据流向kafka
func (b *BinlogKafkaBroker) Pipe(river *river.River, from river.From) error {
	if err := b.Broker.Pipe(river, from); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Consume 消费kafka中的数据
func (b *BinlogKafkaBroker) Consume(fn func(*common.BinlogEvent) error) error {
	consumer := func(msg *sarama.ConsumerMessage) error {
		event := common.BinlogEvent{}
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return errors.Trace(err)
		}
		if err := fn(&event); err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	if err := b.Broker.Consume(consumer); err != nil {
		return errors.Trace(err)
	}
	return nil
}
