with orders as (
    select * from {{ ref('stg_orders') }}
),

items as (
    select
        order_id,
        count(order_item_id) as total_items_count,
        sum(price) as total_order_amount,
        sum(freight_value) as total_freight_amount,
        sum(total_item_value) as total_order_value
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.actual_delivery_days,
    o.is_delayed,
    coalesce(i.total_items_count, 0) as total_items_count,
    coalesce(i.total_order_amount, 0) as total_order_amount,
    coalesce(i.total_freight_amount, 0) as total_freight_amount,
    coalesce(i.total_order_value, 0) as total_order_value
from orders o
left join items i on o.order_id = i.order_id