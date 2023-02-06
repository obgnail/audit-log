package broker

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/audit-log/config"
	"github.com/obgnail/mysql-river/handler/kafka"
)

type KafkaBroker struct {
	binlogTopic string
	txInfoTopic string
	addrs       []string
	producer    sarama.SyncProducer
}

func NewKafkaBroker(addrs []string, binlogTopic string, txInfoTopic string) (*KafkaBroker, error) {
	producer, err := kafka.NewProducer(addrs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	h := &KafkaBroker{producer: producer, binlogTopic: binlogTopic, txInfoTopic: txInfoTopic, addrs: addrs}
	return h, nil
}

func (k *KafkaBroker) PushLog(event *common.BinlogEvent) error {
	result, err := event.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	if _, _, err = kafka.SendMessage(k.producer, k.binlogTopic, result); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (k *KafkaBroker) ConsumeLog(fn func(*common.BinlogEvent) error) error {
	f := func(msg *sarama.ConsumerMessage) error {
		event := common.BinlogEvent{}
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return errors.Trace(err)
		}
		if err := fn(&event); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	if err := kafka.Consume(k.addrs, k.binlogTopic, f); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (k *KafkaBroker) PushTx(txInfo *common.TxInfo) error {
	result, err := txInfo.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	if _, _, err = kafka.SendMessage(k.producer, k.txInfoTopic, result); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (k *KafkaBroker) ConsumeTx(fn func(info *common.TxInfo) error) error {
	f := func(msg *sarama.ConsumerMessage) error {
		info := common.TxInfo{}
		if err := json.Unmarshal(msg.Value, &info); err != nil {
			return errors.Trace(err)
		}
		if err := fn(&info); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	if err := kafka.Consume(k.addrs, k.txInfoTopic, f); err != nil {
		return errors.Trace(err)
	}
	return nil
}

var (
	AuditLogBroker *KafkaBroker
)

func InitBroker() (err error) {
	Kafka := config.Kafka
	AuditLogBroker, err = NewKafkaBroker(Kafka.Addrs, Kafka.BinlogTopic, Kafka.TxInfoTopic)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
