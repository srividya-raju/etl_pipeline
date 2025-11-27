with deduped as (select
        *,
       row_number() over (partition by order_id,listing_id,placed_at,status) as rn
    from  {{ ref('stg_orders') }} as orders
)

select
    listing_id,
    order_id,
    placed_at as order_placed_timestamp,
    date_trunc('hour', cast(placed_at as timestamp)) as rounded_order_placed_timestamp,
    cast(placed_at as date) as order_placed_date,
    status as order_status
from deduped
where rn = 1