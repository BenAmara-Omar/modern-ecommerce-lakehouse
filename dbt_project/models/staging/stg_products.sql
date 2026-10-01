with source as (
    select * from {{ source('silver_sources', 'silver_products') }}
)

select
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g
from source