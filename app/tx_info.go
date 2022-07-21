package app

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/obgnail/audit-log/audit_log"
	"github.com/obgnail/audit-log/compose/clickhouse"
	"github.com/obgnail/audit-log/compose/kafka"
	"github.com/juju/errors"
	"log"
	"time"
)

func Sync() error {
	partitions, err := kafka.Consumer.Partitions(kafka.TxInfoTopic)
	if err != nil {
		return errors.Trace(err)
	}
	// 如果一条 tx_info 轮训了60次（60s）,仍然没有找到完整的 binlog_event 则将这个 tx_info
	// 存入 tx_info 表且状态标记为未完成，sync 继续消费 kafka 中另外的 tx_info message.
	// 另外开一个 goroutine 轮训 tx_info 中过去 72 小时未完成的数据。
	txInfoChan := make(chan *audit_log.TxInfo, 500)
	for _, p := range partitions {
		var offset int64 = 0
		offset, err = useStoredOffsetIfExists(p, offset)
		if err != nil {
			return errors.Trace(err)
		}

		consumer, err := kafka.Consumer.ConsumePartition(kafka.TxInfoTopic, p, offset)
		if err != nil {
			return errors.Trace(err)
		}

		go func(pc sarama.PartitionConsumer) {
			defer pc.AsyncClose()
			for msg := range pc.Messages() {
				txInfo := new(audit_log.TxInfo)
				if err := json.Unmarshal(msg.Value, txInfo); err != nil {
					log.Printf("[err] %s", err)
					continue
				}
				txInfoChan <- txInfo

			}
		}(consumer)
	}

	for txInfo := range txInfoChan {
		err = HandleTxInfo(txInfo)
		if err != nil {
			log.Printf("[err] %s", err)
		}
	}
	return nil
}

func useStoredOffsetIfExists(partition int32, offset int64) (int64, error) {
	offsetByte, err := kafka.KafkaOffset.Get(kafka.TxInfoTopic, partition)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(offsetByte) > 0 {
		offset = int64(binary.LittleEndian.Uint64(offsetByte))
	}
	return offset, nil
}

func HandleTxInfo(txInfo *audit_log.TxInfo) error {
	if txInfo.GTID == "" {
		return nil
	}
	exist, err := clickhouse.AuditLogExist(clickhouse.CH, txInfo.TxTime, txInfo.GTID)
	if err != nil {
		return errors.Trace(err)
	}
	if exist {
		return nil
	}

	binlogEvents, err := listBinlogEvents(txInfo.GTID)
	if err != nil {
		return errors.Trace(err)
	}
	if len(binlogEvents) == 0 {
		log.Printf("binlog not found for tx: %s", txInfo.GTID)
	}
	// 历史未处理的数据不需要再次保存
	err = kafka.KafkaOffset.Put(txInfo.Msg.Topic, txInfo.Msg.Partition, txInfo.Msg.Offset)
	if err != nil {
		return errors.Trace(err)
	}
	fmt.Println("--- handle binlog", txInfo)
	return nil
}

func listBinlogEvents(gtid string) ([]audit_log.BinlogEvent, error) {
	binlogEvents, err := clickhouse.ListTxBinlogEvent(clickhouse.CH, gtid)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(binlogEvents) > 0 {
		return binlogEvents, nil
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	var tickerCount uint16
	for {
		select {
		case <-ticker.C:
			binlogEvents, err = clickhouse.ListTxBinlogEvent(clickhouse.CH, gtid)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(binlogEvents) == 0 {
				tickerCount++
				if tickerCount >= 3 {
					return nil, nil
				}
				continue
			}
			return binlogEvents, nil
		}
	}
}
