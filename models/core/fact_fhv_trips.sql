{{ config(materialized='table') }}

with fhv_data as (
    select * from {{ ref('fhv_data') }}
    where PUlocationID is not Null and DOlocationID is not Null
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_data.tripid, 
    fhv_data.dispatching_base_num, 
    
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    
    fhv_data.PUlocationID as pickup_locationid,

    fhv_data.pickup_datetime as pickup_datetime,
    fhv_data.dropOff_datetime as dropOff_datetime,
    
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    
    fhv_data.DOlocationID as droppoff_locationid,
    
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.DOlocationID = dropoff_zone.locationid
