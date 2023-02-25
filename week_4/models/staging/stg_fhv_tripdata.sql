{{ config(materialized='view') }}

select
    *
from {{ source('staging','fhv_trips_2019_partitoned_clustered') }}