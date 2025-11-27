with deduped as (select
        *,
       row_number() over (partition by id,name,timestamp) as rn
    from  {{ ref('stg_org') }}
)

select
   id as org_id,
   	"name" as org_name,
    "timestamp" as org_start_date
from deduped
where rn = 1