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

CREATE TABLE audit_log
(
    `gtid`           String,
    `context`        String,
    `operator_uuid`  String,
    `action`         Int32,
    `action_context` Int32,
    `object_type`    String,
    `data`           String,
    `ip`             Nullable(IPv4),
    `tx_time`        DateTime64(3, 'Asia/Shanghai')
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(tx_time) ORDER BY (toDateTime(tx_time), context, action, operator_uuid);
