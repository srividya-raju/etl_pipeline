with deduped as (select
        *,
       row_number() over (partition by id, outlet_id, platform_id) as rn
    from  {{ ref('stg_listing') }} 
)

select
   id as listing_id,
        outlet_id,
        platform_id,
        cast(timestamp as date) as listing_date
from deduped
where rn = 1