{{ config(materialized='view') }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['int64_field_0', 'pickup_datetime']) }} as tripid,
    int64_field_0,
    dispatching_base_num,    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,


    -- location info
    cast(PUlocationID as integer) as PUlocationID,
    cast(DOlocationID as integer) as DOlocationID

from {{source('staging', 'fhv_2019csv_internal')}}


{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
