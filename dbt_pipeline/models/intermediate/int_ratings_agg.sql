with deduped as (select
        *,
       row_number() over (partition by "date",listing_id,cnt_ratings,avg_rating) as rn
    from  {{ ref('stg_ratings_agg') }}
),
transformed as (
select
   cast("date" as date) as date_of_rating,
   listing_id,
   	cnt_ratings,
    cast(avg_rating as numeric) as avg_rating
from deduped
where rn = 1)

select date_of_rating, listing_id, sum(cnt_ratings) as cnt_ratings,	avg(avg_rating) as avg_rating
from transformed
group by date_of_rating, listing_id