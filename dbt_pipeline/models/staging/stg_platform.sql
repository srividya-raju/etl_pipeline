with stg as (select  *
from {{source('datalake','platform')}}
)
select * from stg