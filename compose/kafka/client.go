package kafka

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/audit_log"
	"github.com/obgnail/audit-log/config"
	"log"
	"sync"
	"time"
)

var (
	Producer    *Client
	Consumer    sarama.Consumer
	TxInfoTopic string
)

func InitConsumer() error {
	brokers := config.StringSlice("kafka_brokers")
	if len(brokers) == 0 {
		return fmt.Errorf("kafka_brokers is empty")
	}
	TxInfoTopic = config.String("kafka_tx_info_topic", "kafka_tx_info_topic")
	cfg := sarama.NewConfig()
	cfg.Metadata.Retry.Backoff = 1 * time.Second
	cfg.Metadata.Retry.Max = 10
	cfg.Metadata.Timeout = 20 * time.Second

	var err error
	Consumer, err = sarama.NewConsumer(brokers, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func InitClient() error {
	brokers := config.StringSlice("kafka_brokers")
	if len(brokers) == 0 {
		return fmt.Errorf("kafka_brokers is empty")
	}
	txInfoTopic := config.String("kafka_tx_info_topic", "kafka_tx_info_topic")
	binlogTopic := config.String("kafka_binlog_topic", "kafka_binlog_topic")
	Producer = NewClient(brokers, txInfoTopic, binlogTopic)
	if err := Producer.InitProducer(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func InitKafka() (err error) {
	if err = InitClient(); err != nil {
		return errors.Trace(err)
	}
	if err = InitConsumer(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type Client struct {
	producer    sarama.SyncProducer
	txInfoTopic string
	binlogTopic string
	brokers     []string
	sync.Mutex
	configured bool
}

func NewClient(brokers []string, txInfoTopic string, binlogTopic string) *Client {
	client := &Client{
		brokers:     brokers,
		txInfoTopic: txInfoTopic,
		binlogTopic: binlogTopic,
		configured:  len(brokers) > 0 && txInfoTopic != "",
	}
	return client
}

func (c *Client) InitProducer() error {
	if !c.Configured() {
		return nil
	}

	cfg := sarama.NewConfig()
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 1000 * time.Millisecond
	cfg.Producer.Return.Successes = true
	cfg.Producer.Compression = sarama.CompressionLZ4
	cfg.Producer.MaxMessageBytes = 1024 * 1024 * 2 // 2Mb
	cfg.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	producer, err := sarama.NewSyncProducer(c.brokers, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	c.producer = producer
	return nil
}

func (c *Client) Configured() bool {
	return c.configured
}

func (c *Client) GetAuditLogTopic() string {
	return c.txInfoTopic
}

func (c *Client) GetProducer() sarama.SyncProducer {
	if c.producer != nil {
		return c.producer
	}
	if !c.Configured() {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	for i := 0; c.producer == nil && i < 3; i++ {
		log.Printf("init producer retry: %d", i)
		if err := c.InitProducer(); err != nil {
			log.Printf("init producer failed: %s", err)
			time.Sleep(time.Second)
		}
	}
	if c.producer == nil {
		log.Println("get producer failed")
	}
	return c.producer
}

func (c *Client) SendTxInfoMessage(ctx, userUUID, Gtid string) error {
	producer := c.GetProducer()
	if producer == nil {
		return nil
	}
	txInfo := audit_log.NewTxInfo(ctx, userUUID, Gtid)
	err := SendSyncTxInfoMessage(producer, c.txInfoTopic, txInfo)
	return errors.Trace(err)
}

func (c *Client) SendBinlogMessage(db, table, gtid string, action int, eventTime uint32, event sql.RawBytes) error {
	producer := c.GetProducer()
	if producer == nil {
		return nil
	}
	binlogEvent := audit_log.NewBinlogEvent(db, table, gtid, action, eventTime, event)
	err := SendSyncBinlogMessage(producer, c.binlogTopic, binlogEvent)
	return errors.Trace(err)
}

func SendSyncBinlogMessage(producer sarama.SyncProducer, topic string, binlog *audit_log.BinlogEvent) error {
	binlogEvent, err := json.Marshal(binlog)
	if err != nil {
		return errors.Trace(err)
	}

	msg := new(sarama.ProducerMessage)
	msg.Topic = topic
	msg.Value = sarama.ByteEncoder(binlogEvent)
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func SendSyncTxInfoMessage(producer sarama.SyncProducer, topic string, txInfo *audit_log.TxInfo) error {
	byteTxInfo, err := json.Marshal(txInfo)
	if err != nil {
		return errors.Trace(err)
	}

	msg := new(sarama.ProducerMessage)
	msg.Topic = topic
	msg.Value = sarama.ByteEncoder(byteTxInfo)
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
