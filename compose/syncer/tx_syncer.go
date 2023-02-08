package syncer

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/broker"
	"github.com/obgnail/audit-log/compose/clickhouse"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/audit-log/config"
	"github.com/obgnail/mysql-river/river"
	"time"
)

type TxInfoSynchronizer struct {
	*broker.TxKafkaBroker
}

func NewTxInfoSyncer(broker *broker.TxKafkaBroker) *TxInfoSynchronizer {
	return &TxInfoSynchronizer{TxKafkaBroker: broker}
}

type unprocessedInfos struct {
	minTime      time.Time
	gtidArr      []string
	mapGtid2Info map[string]clickhouse.TxInfo
}

func (s *TxInfoSynchronizer) getUnprocessedInfos() (*unprocessedInfos, error) {
	infoList, err := clickhouse.ListUnprocessedTxInfo()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(infoList) == 0 {
		return nil, errors.Trace(err)
	}

	result := &unprocessedInfos{
		minTime:      infoList[0].Time,
		gtidArr:      make([]string, len(infoList)),
		mapGtid2Info: make(map[string]clickhouse.TxInfo, len(infoList)),
	}

	for i := 0; i < len(infoList); i++ {
		if infoList[i].Time.UnixNano() < result.minTime.UnixNano() {
			result.minTime = infoList[i].Time
		}
		result.gtidArr[i] = infoList[i].GTID
		result.mapGtid2Info[infoList[i].GTID] = infoList[i]
	}

	return result, nil
}

// TODO check gtid in audit_log table
func (s *TxInfoSynchronizer) filterInfoInAuditLog(infos *unprocessedInfos) (*unprocessedInfos, error) {
	return nil, nil
}

func (s *TxInfoSynchronizer) getToProcessIxInfos(infos *unprocessedInfos) ([]clickhouse.TxInfoBinlogEvent, error) {
	binlogEvents, err := clickhouse.ListBinlogEvents(infos.gtidArr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	mapGtidBinlogEvents := make(map[string][]clickhouse.BinlogEvent)
	for _, event := range binlogEvents {
		mapGtidBinlogEvents[event.GTID] = append(mapGtidBinlogEvents[event.GTID], event)
	}

	txInfoBinlogEvents := make([]clickhouse.TxInfoBinlogEvent, 0)
	processedTxInfo := make([]clickhouse.TxInfo, 0)
	for gtid, events := range mapGtidBinlogEvents {
		txInfo, found := infos.mapGtid2Info[gtid]
		if !found {
			continue
		}
		txInfoBinlogEvents = append(txInfoBinlogEvents, clickhouse.TxInfoBinlogEvent{
			Time:         txInfo.Time,
			GTID:         txInfo.GTID,
			Context:      txInfo.Context,
			BinlogEvents: events,
		})
		txInfo.Status = clickhouse.StatusTxInfoProcessed
		processedTxInfo = append(processedTxInfo, txInfo)
	}
	return txInfoBinlogEvents, nil
}

func (s *TxInfoSynchronizer) handleUnprocessedTxInfo() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			infos, err := s.getUnprocessedInfos()
			if err != nil {
				river.Logger.Errorf("ListUnprocessedTxInfo error: %s", err.Error())
				continue
			}
			if len(infos.gtidArr) == 0 {
				continue
			}

			infos, err = s.filterInfoInAuditLog(infos)
			if err != nil {
				river.Logger.Errorf("filterInfoInAuditLog error: %s", err.Error())
				continue
			}
			if len(infos.gtidArr) == 0 {
				continue
			}

			infoEvents, err := s.getToProcessIxInfos(infos)
			if err != nil {
				river.Logger.Errorf("getToProcessIxInfos error: %s", err.Error())
				continue
			}

			// TODO
			fmt.Println(infoEvents)
		}
	}
}

func (s *TxInfoSynchronizer) handleFoundNoEventTxInfo(info *common.TxInfo) error {
	river.Logger.Warn("binlog not found for tx: %s", info.GTID)
	chInfo := clickhouse.ConvertTx(info)
	chInfo.Status = clickhouse.StatusTxInfoUnprocessed
	if err := clickhouse.InsertTxInfo(chInfo); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// 如果一条 tx_info 轮训了60次（60s）, 仍然没有找到完整的 binlog_event 则将这个 tx_info
// 存入 tx_info 表且状态标记为未完成，sync 继续消费 kafka 中另外的 tx_info message.
// 另外开一个 goroutine 轮训 tx_info 中过去 72 小时未完成的数据。
func (s *TxInfoSynchronizer) sync() error {
	go s.handleUnprocessedTxInfo()
	err := s.TxKafkaBroker.Consume(func(info *common.TxInfo) error {
		binlogEvents, err := listBinlogEvents(info.GTID)
		if err != nil {
			return errors.Trace(err)
		}
		if len(binlogEvents) == 0 {
			if err := s.handleFoundNoEventTxInfo(info); err != nil {
				return errors.Trace(err)
			}
			return nil
		}
		txBinlogEvent := clickhouse.TxInfoBinlogEvent{
			Time:         time.Unix(info.Time, 0),
			Context:      info.Context,
			GTID:         info.GTID,
			BinlogEvents: binlogEvents,
		}
		// TODO
		fmt.Println(txBinlogEvent)

		chInfo := clickhouse.ConvertTx(info)
		chInfo.Status = clickhouse.StatusTxInfoProcessed
		err = clickhouse.InsertTxInfo(chInfo)
		return nil
	})
	return errors.Trace(err)
}

func listBinlogEvents(gtid string) ([]clickhouse.BinlogEvent, error) {
	binlogEvents, err := clickhouse.ListBinlogEvent(gtid)
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
			binlogEvents, err = clickhouse.ListBinlogEvent(gtid)
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

var (
	TxInfoSyncer *TxInfoSynchronizer
)

func InitTxInfoSyncer() (err error) {
	TxInfoBroker, err := broker.NewTxKafkaBroker(config.Kafka.Addrs, config.Kafka.TxInfoTopic)
	if err != nil {
		return errors.Trace(err)
	}
	TxInfoSyncer = NewTxInfoSyncer(TxInfoBroker)
	return nil
}
