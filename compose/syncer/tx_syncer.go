package syncer

import (
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/broker"
	"github.com/obgnail/audit-log/compose/clickhouse"
	"github.com/obgnail/audit-log/compose/common"
	"github.com/obgnail/audit-log/compose/logger"
	"github.com/obgnail/audit-log/config"
	"time"
)

const (
	defaultAuditChanSize = 1024

	defaultRecheckInterval = 10 * time.Second

	defaultGetBinlogInterval = 1 * time.Second
	defaultGenBinlogMaxRetry = 3
)

type TxInfoSynchronizer struct {
	*broker.TxKafkaBroker

	auditChan chan clickhouse.TxInfoBinlogEvent
}

func NewTxInfoSyncer(broker *broker.TxKafkaBroker) *TxInfoSynchronizer {
	return &TxInfoSynchronizer{
		TxKafkaBroker: broker,
		auditChan:     make(chan clickhouse.TxInfoBinlogEvent, defaultAuditChanSize),
	}
}

func (s *TxInfoSynchronizer) HandleAuditLog(fn func(txEvent clickhouse.TxInfoBinlogEvent) error) {
	for audit := range s.auditChan {
		if err := fn(audit); err != nil {
			logger.ErrorDetails(errors.Trace(err))
		}
	}
}

func (s *TxInfoSynchronizer) handleUnprocessedTxInfo() {
	ticker := time.NewTicker(defaultRecheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			infos, err := getUnprocessedInfos()
			if err != nil {
				logger.ErrorDetails(errors.Trace(err))
				continue
			}
			if infos.Empty() {
				continue
			}

			toProcessInfoEvents, toProcessInfos, err := infos.getToProcess()
			if err != nil {
				logger.ErrorDetails(errors.Trace(err))
				continue
			}

			for _, audit := range toProcessInfoEvents {
				s.auditChan <- audit
			}

			if err := clickhouse.BatchInsertTxInfo(toProcessInfos); err != nil {
				logger.ErrorDetails(errors.Trace(err))
				continue
			}
		}
	}
}

func (s *TxInfoSynchronizer) handleFoundNoEventTxInfo(info *common.TxInfo) error {
	logger.Warn("binlog not found for tx: %s", info.GTID)
	chInfo := clickhouse.ConvertCHFormatTxInfo(info, clickhouse.StatusTxInfoUnprocessed)
	if err := clickhouse.InsertTxInfo(chInfo); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// processTxInfo 如果一条 tx_info 轮训了3次（3s）, 仍然没有找到完整的 binlog_event 则将这个 tx_info
// 存入 tx_info 表且状态标记为未完成，processTxInfo 继续消费 kafka 中另外的 tx_info message.
// 另外开一个 goroutine 轮训 tx_info 中过去 72 小时未完成的数据。
func (s *TxInfoSynchronizer) processTxInfo(info *common.TxInfo) error {
	events, err := tryListBinlogEvents(info.GTID)
	if err != nil {
		return errors.Trace(err)
	}
	if len(events) == 0 {
		if err := s.handleFoundNoEventTxInfo(info); err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	chInfo := clickhouse.ConvertCHFormatTxInfo(info, clickhouse.StatusTxInfoProcessed)
	infoEvents := clickhouse.CombineTxAndEvents(chInfo, events)

	s.auditChan <- infoEvents

	err = clickhouse.InsertTxInfo(chInfo)
	return nil
}

// Sync 获取kafka中的txInfo数据,根据gtid从clickhouse中获取对应的binlogEvent
// 然后将二者组合,流入auditChan,最后将txInfo存入clickhouse
func (s *TxInfoSynchronizer) Sync() {
	go s.handleUnprocessedTxInfo()

	err := s.TxKafkaBroker.Consume(s.processTxInfo)
	if err != nil {
		logger.ErrorDetails(errors.Trace(err))
	}
}

func tryListBinlogEvents(gtid string) ([]clickhouse.BinlogEvent, error) {
	events, err := clickhouse.ListBinlogEvent(gtid)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(events) > 0 {
		return events, nil
	}

	ticker := time.NewTicker(defaultGetBinlogInterval)
	defer ticker.Stop()

	var retry uint16
	for {
		select {
		case <-ticker.C:
			events, err = clickhouse.ListBinlogEvent(gtid)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(events) == 0 {
				retry++
				if retry >= defaultGenBinlogMaxRetry {
					return nil, nil
				}
				continue
			}
			return events, nil
		}
	}
}

type unprocessedInfos struct {
	minTime      time.Time
	gtidArr      []string
	mapGtid2Info map[string]clickhouse.TxInfo
}

func getUnprocessedInfos() (*unprocessedInfos, error) {
	infos, err := clickhouse.ListUnprocessedTxInfo()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(infos) == 0 {
		return nil, nil
	}

	result := &unprocessedInfos{
		minTime:      infos[0].Time,
		gtidArr:      make([]string, len(infos)),
		mapGtid2Info: make(map[string]clickhouse.TxInfo, len(infos)),
	}

	for _, info := range infos {
		result.Add(info)
	}
	return result, nil
}

func (i *unprocessedInfos) Empty() bool {
	return i == nil || len(i.gtidArr) == 0
}

func (i *unprocessedInfos) Add(info clickhouse.TxInfo) {
	if info.Time.UnixNano() < i.minTime.UnixNano() {
		i.minTime = info.Time
	}
	i.gtidArr = append(i.gtidArr, info.GTID)
	i.mapGtid2Info[info.GTID] = info
}

func (i *unprocessedInfos) getToProcess() (
	toProcessInfoEvents []clickhouse.TxInfoBinlogEvent,
	toProcessInfo []clickhouse.TxInfo,
	err error,
) {
	toProcessEvents, err := clickhouse.ListBinlogEvents(i.gtidArr)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	mapGtid2Events := make(map[string][]clickhouse.BinlogEvent)
	for _, event := range toProcessEvents {
		mapGtid2Events[event.GTID] = append(mapGtid2Events[event.GTID], event)
	}

	for gtid, gEvents := range mapGtid2Events {
		info, found := i.mapGtid2Info[gtid]
		if !found {
			continue
		}

		toProcessInfoEvent := clickhouse.CombineTxAndEvents(info, gEvents)
		toProcessInfoEvents = append(toProcessInfoEvents, toProcessInfoEvent)

		info.Status = clickhouse.StatusTxInfoProcessed
		toProcessInfo = append(toProcessInfo, info)
	}
	return toProcessInfoEvents, toProcessInfo, nil
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
