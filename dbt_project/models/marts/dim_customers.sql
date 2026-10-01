with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders_summary as (
    select
        customer_id,
        count(order_id) as total_orders,
        min(order_purchase_timestamp) as first_order_date,
        max(order_purchase_timestamp) as last_order_date
    from orders
    group by customer_id
)

select
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    coalesce(s.total_orders, 0) as total_orders,
    s.first_order_date,
    s.last_order_date
from customers c
left join customer_orders_summary s on c.customer_id = s.customer_id