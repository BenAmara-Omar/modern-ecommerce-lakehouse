with source as (
    select * from {{ source('silver_sources', 'silver_orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    actual_delivery_days,
    is_delayed
from source