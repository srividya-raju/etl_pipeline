select l.listing_id,
-- listing_date,
order_id,
order_placed_timestamp,
rounded_order_placed_timestamp,
order_placed_date,
order_status,
p.*,
ou.outlet_id,
ou.org_id,
outlet_name,
latitude,
longitude,
--outlet_start_timestamp,
org.org_name,
org_start_date,
rank_date,
is_online,
avg_rank,
ra.date_of_rating,
ra.cnt_ratings,
ra.avg_rating,
od.order_date,
od.count_orders,
api_timestamp ,
 temperature_2m,
  relative_humidity_2m,
   wind_speed_10m
from {{ref("int_listing")}} l
right join {{ref("int_orders")}} o 
 on l.listing_id=o.listing_id
left join {{ref("int_platform")}} p
 on p.platform_id=l.platform_id
 left join {{ref("int_outlet")}} ou 
 on l.outlet_id=ou.outlet_id
  left join {{ref("int_org")}} org
 on org.org_id=ou.org_id
 left join {{ref("int_rank")}} r
 on l.listing_id=r.listing_id
 and o.order_placed_date=r.rank_date
 left join {{ref("int_ratings_agg")}} ra
 on l.listing_id=ra.listing_id
 and o.order_placed_date=ra.date_of_rating
 left join {{ref("int_orders_daily")}} od
 on l.listing_id=od.listing_id
 and od.order_date=o.order_placed_date
 left join {{ref("int_api_data")}} api
 on api.outlet_id=ou.outlet_id
 and o.rounded_order_placed_timestamp=api.api_timestamp