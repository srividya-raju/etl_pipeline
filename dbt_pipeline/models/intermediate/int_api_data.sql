with deduped as (select
        *,
       row_number() over (partition by outlet_id, timestamp,temperature_2m,relative_humidity_2m,wind_speed_10m) as rn
    from  {{ ref('stg_api_data') }} 
)


select
  outlet_id,
  	"timestamp" as api_timestamp,
    	temperature_2m,
        	relative_humidity_2m,
            	wind_speed_10m
from deduped
where rn = 1
