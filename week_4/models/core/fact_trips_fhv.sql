{{ config(materialized='table') }}


with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    t.*,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from {{ ref('stg_fhv_tripdata') }} t
inner join dim_zones as pickup_zone on t.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone on t.DOlocationID = dropoff_zone.locationid