with deduped as (select
        *,
       row_number() over (partition by date,listing_id,rank,timestamp,is_online) as rn
    from  {{ ref('stg_rank') }}
),
transformed as (
select
   listing_id,
   cast("date" as date) as rank_date,
   	"timestamp"	rank_timestamp,
    is_online,
    "rank" as rank_value
from deduped
where rn = 1)

select listing_id,rank_date,is_online, avg(cast(rank_value as numeric)) as avg_rank
from transformed
where is_online =true
group by listing_id,rank_date,is_online