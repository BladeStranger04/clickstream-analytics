create database if not exists analytics;

create table if not exists analytics.raw_events
(
    event_id String,
    user_id UInt64,
    session_id String,
    event_type LowCardinality(String),
    url String,
    device_type LowCardinality(String),
    event_time DateTime,
    event_date Date default toDate(event_time)
)
engine = MergeTree
partition by toYYYYMM(event_date)
order by (event_date, user_id, event_time, event_id);
