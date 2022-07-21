CREATE TABLE tx_info
(
    `gtid`           String,
    `context`        String,
    `user_uuid`      String,
    `tx_time`        DateTime(3, 'Asia/Shanghai'),
    `status`         UInt8
) ENGINE = ReplacingMergeTree()
      PARTITION BY toYYYYMM(tx_time) ORDER BY gtid
      TTL toDateTime(tx_time) + INTERVAL 60 DAY;


CREATE TABLE binlog_event
(
    `event_db`     String,
    `event_table`  String,
    `event_action` Int8,
    `gtid`         String,
    `event`        String,
    `event_time`   DateTime DEFAULT now()
) ENGINE = MergeTree()
      PARTITION BY toYYYYMMDD(event_time) ORDER BY gtid
      TTL event_time + INTERVAL 30 DAY;


CREATE TABLE audit_log
(
    `gtid`           String,
    `context`        String,
    `operator_uuid`  String,
    `action`         Int32,
    `action_context` Int32,
    `object_type`    String,
    `data`           String,
    `tx_time`        DateTime(3, 'Asia/Shanghai')
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(tx_time) ORDER BY (toDateTime(tx_time), context, action, operator_uuid);

-- CREATE TABLE obj_mapping
-- (
--     `key`          String,
--     `name`         Nullable(String),
--     `field_values` Nullable(String)
-- ) ENGINE = EmbeddedRocksDB PRIMARY KEY (key);
