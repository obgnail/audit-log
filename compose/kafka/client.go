package kafka

//import (
//	"fmt"
//	"github.com/Shopify/sarama"
//	"github.com/juju/errors"
//	"github.com/obgnail/audit-log/config"
//	"sync"
//)
//
//var (
//	producer sarama.SyncProducer
//	consumer sarama.Consumer
//)
//
//var (
//	kafkaHost        = config.String("kafka_host", "127.0.0.1")
//	kafkaPort        = config.Int("kafka_port", 9202)
//	kafkaAddr        = fmt.Sprintf("%s:%d", kafkaHost, kafkaPort)
//	kafkaBinlogTopic = config.String("kafka_binlog_topic", "kafka_binlog_topic")
//	kafkaTxInfoTopic = config.String("kafka_tx_info_topic", "kafka_tx_info_topic")
//)
//
//func NewProducer(addrs []string) (sarama.SyncProducer, error) {
//	cfg := sarama.NewConfig()
//	cfg.Producer.RequiredAcks = sarama.WaitForAll
//	//cfg.Producer.Partitioner = sarama.NewRandomPartitioner
//	cfg.Producer.Return.Errors = true
//	cfg.Producer.Return.Successes = true
//
//	client, err := sarama.NewSyncProducer(addrs, cfg)
//	if err != nil {
//		return nil, errors.Trace(err)
//	}
//	return client, nil
//}
//
//func SendToBinlog(content []byte) (partition int32, offset int64, err error) {
//	return SendMessage(producer, kafkaBinlogTopic, content)
//}
//
//func SendToTxInfo(content []byte) (partition int32, offset int64, err error) {
//	return SendMessage(producer, kafkaTxInfoTopic, content)
//}
//
//func ConsumeBinlog(f func(msg *sarama.ConsumerMessage) error) error {
//	return Consume(consumer, kafkaBinlogTopic, f)
//}
//
//func ConsumeTxInfo(f func(msg *sarama.ConsumerMessage) error) error {
//	return Consume(consumer, kafkaTxInfoTopic, f)
//}
//
//func SendMessage(producer sarama.SyncProducer, topic string, content []byte) (partition int32, offset int64, err error) {
//	msg := &sarama.ProducerMessage{
//		Topic: topic,
//		Value: sarama.ByteEncoder(content),
//	}
//	partition, offset, err = producer.SendMessage(msg)
//	if err != nil {
//		err = errors.Trace(err)
//		return
//	}
//	return
//}
//
//func Consume(consumer sarama.Consumer, topic string, f func(msg *sarama.ConsumerMessage) error) error {
//	partitionList, err := consumer.Partitions(topic)
//	if err != nil {
//		return errors.Trace(err)
//	}
//
//	var wg sync.WaitGroup
//	for p := range partitionList {
//		pc, err := consumer.ConsumePartition(topic, int32(p), sarama.OffsetNewest)
//		if err != nil {
//			return errors.Trace(err)
//		}
//		defer pc.AsyncClose()
//
//		wg.Add(1)
//		go func(sarama.PartitionConsumer) {
//			for msg := range pc.Messages() {
//				if err := f(msg); err != nil {
//					return
//				}
//			}
//		}(pc)
//	}
//	wg.Wait()
//	return nil
//}
//
//func init() {
//	addr := []string{kafkaAddr}
//	var err error
//	producer, err = NewProducer(addr)
//	if err != nil {
//		panic(err)
//	}
//	consumer, err = sarama.NewConsumer(addr, nil)
//	if err != nil {
//		panic(err)
//	}
//}
