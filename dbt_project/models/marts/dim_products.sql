with products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    product_category_name,
    product_weight_g
from products