with stg as (select  *
from {{source('datalake','orders')}}
)
select * from stg