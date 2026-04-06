CREATE DATABASE IF NOT EXISTS events_bench;

USE events_bench;

CREATE TABLE IF NOT EXISTS events (
    id UUID DEFAULT generateUUIDv4(),
    start DateTime64(3),
    end DateTime64(3),
    type UInt8,
    created_at DateTime64(3) DEFAULT now64(),
    updated_at DateTime64(3) DEFAULT now64(),
) ENGINE = MergeTree()
ORDER BY (start, id)
TTL toDateTime(start) + INTERVAL 1 YEAR;

