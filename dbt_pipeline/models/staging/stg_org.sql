with stg as (select  *
from {{source('datalake','org')}}
)
select * from stg