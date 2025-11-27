 with deduped as (select
        *,
       row_number() over (partition by date,listing_id,orders) as rn
    from  {{ ref('stg_orders_daily') }} 
) 
select
   listing_id,	
   cast("date" as date) as order_date,
    sum(orders) as count_orders
   -- "timestamp"	as orders_daily_timestamp
from deduped
 where rn = 1
group by listing_id,order_date

