CREATE OR REPLACE EXTERNAL TABLE `datazc-pm.taxi_trips.fhv_trips_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_datazc-pm/data/fhv_tripdata_2019-*.csv.gz']
);

select 
count(*) 
from `datazc-pm.taxi_trips.fhv_trips_2019`
;

CREATE OR REPLACE TABLE datazc-pm.taxi_trips.fhv_trips_2019_non_partitoned AS
SELECT * FROM datazc-pm.taxi_trips.fhv_trips_2019
;

select
count(distinct affiliated_base_number)
from datazc-pm.taxi_trips.fhv_trips_2019
;

select
count(distinct affiliated_base_number)
from datazc-pm.taxi_trips.fhv_trips_2019_non_partitoned
;

select
count(*)
from datazc-pm.taxi_trips.fhv_trips_2019_non_partitoned
where PUlocationID is null
  and DOlocationID is null


CREATE OR REPLACE TABLE datazc-pm.taxi_trips.fhv_trips_2019_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM datazc-pm.taxi_trips.fhv_trips_2019
;

select
distinct affiliated_base_number
from datazc-pm.taxi_trips.fhv_trips_2019_non_partitoned
where pickup_datetime between '2019-03-01' and '2019-04-01'
