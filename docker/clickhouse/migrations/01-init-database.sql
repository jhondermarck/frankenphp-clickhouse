CREATE DATABASE IF NOT EXISTS events_db;

USE events_db;

CREATE TABLE IF NOT EXISTS events (
    id UUID DEFAULT generateUUIDv4(),
    start_date DateTime64(3),
    end_date DateTime64(3),
    type UInt8,
    created_at DateTime64(3) DEFAULT now64(),
    updated_at DateTime64(3) DEFAULT now64(),
    duration UInt32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(start_date)
ORDER BY (start_date, id)
TTL toDateTime(start_date) + INTERVAL 1 YEAR;

CREATE INDEX IF NOT EXISTS idx_events_type ON events (type) TYPE set(100) GRANULARITY 4;
