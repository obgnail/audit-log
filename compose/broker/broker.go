package broker

import (
	"github.com/obgnail/audit-log/compose/common"
)

type BinlogBroker interface {
	PushLog(event *common.BinlogEvent) error
	ConsumeLog(fn func(*common.BinlogEvent) error) error
}

type TxInfoBroker interface {
	PushTx(txInfo *common.TxInfo) error
	ConsumeTx(fn func(info *common.TxInfo) error) error
}
