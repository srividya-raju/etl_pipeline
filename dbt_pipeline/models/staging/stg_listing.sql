with stg as (select  *
from {{source('datalake','listing')}}
)
select  *
from stg