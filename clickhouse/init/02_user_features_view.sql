create view if not exists analytics.user_features as
select
    user_id,
    event_date,
    count() as clicks_count,
    countIf(event_type = 'view_product') as product_views,
    countIf(event_type = 'add_to_cart') as cart_adds,
    countIf(event_type = 'purchase') as purchases,
    max(device_type = 'mobile') as is_mobile,
    greatest(dateDiff('second', min(event_time), max(event_time)), 1) as session_duration
from analytics.raw_events
group by user_id, event_date;
