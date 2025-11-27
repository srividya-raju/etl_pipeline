with stg as (select  *
from {{source('dbttest','api_data')}}
)
select  *
from stg