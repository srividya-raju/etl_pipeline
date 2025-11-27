with deduped as (select
        *,
       row_number() over (partition by id,org_id,name,latitude,longitude) as rn --removed timestamp
    from  {{ ref('stg_outlet') }}
)

select
   id as outlet_id,
   	org_id,
    "name"	as outlet_name,
    coalesce(nullif(latitude,'')::numeric(5,2),0) as latitude,
    coalesce(nullif(longitude,'')::numeric(5,2),0)  as longitude
    --"timestamp" as outlet_start_timestamp	
from deduped
where rn = 1