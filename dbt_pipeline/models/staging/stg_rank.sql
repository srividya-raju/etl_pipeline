with stg as (select  *
from {{source('datalake','rank')}}
)
select * from stg