with stg as (select  *
from {{source('datalake','outlet')}}
)
select * from stg