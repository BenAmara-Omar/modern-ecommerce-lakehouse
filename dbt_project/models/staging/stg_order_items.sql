with source as (
    select * from {{ source('silver_sources', 'silver_order_items') }}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    total_item_value
from source