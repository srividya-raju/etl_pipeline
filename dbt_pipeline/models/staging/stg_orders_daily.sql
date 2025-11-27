with stg as (select  *
from {{source('datalake','orders_daily')}}
)
select * from stg