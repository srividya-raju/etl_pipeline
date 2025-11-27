select
   id as platform_id,
   "group" as platform_group,
   	"name" as platform_name,
    country
from {{ref('stg_platform')}}
