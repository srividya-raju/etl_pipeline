with stg as (select  *
from {{source('datalake','ratings_agg')}}
)
select * from stg