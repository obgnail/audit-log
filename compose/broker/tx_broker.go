package broker

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/mysql-river/handler/kafka"
)

type TxKafkaBroker struct {
	txInfoTopic string
	addrs       []string
	producer    sarama.SyncProducer
}

func NewTxKafkaBroker(addrs []string, txInfoTopic string) (*TxKafkaBroker, error) {
	producer, err := kafka.NewProducer(addrs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	h := &TxKafkaBroker{producer: producer, txInfoTopic: txInfoTopic, addrs: addrs}
	return h, nil
}

func (k *TxKafkaBroker) PushTx(txInfo *common.TxInfo) error {
	result, err := txInfo.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	if _, _, err = kafka.SendMessage(k.producer, k.txInfoTopic, result); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (k *TxKafkaBroker) Consume(fn func(info *common.TxInfo) error) error {
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
	if err := kafka.Consume(k.addrs, k.txInfoTopic, kafka.NewestOffsetGetter, f); err != nil {
		return errors.Trace(err)
	}
	return nil
}
