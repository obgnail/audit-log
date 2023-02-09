CREATE TABLE binlog_event
(
    `db`     String,
    `table`  String,
    `action` Int32,
    `gtid`   String,
    `data`   String,
    `time`   DateTime DEFAULT now()
) ENGINE = MergeTree()
      PARTITION BY toYYYYMMDD(time) ORDER BY gtid
      TTL time + INTERVAL 30 DAY;

CREATE TABLE tx_info
(
    `gtid`    String,
    `context` String,
    `time`    DateTime64(3, 'Asia/Shanghai'),
    `status`  UInt8
) ENGINE = ReplacingMergeTree()
      PARTITION BY toYYYYMM(time) ORDER BY gtid
      TTL toDateTime(time) + INTERVAL 60 DAY;

