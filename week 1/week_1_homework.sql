# Question 3. Count records
select 
  count(*)
from green_2019_hw gh 
where 1=1
  and lpep_dropoff_datetime between '2019-01-15 00:00:00' and '2019-01-15 23:59:59'
  and lpep_pickup_datetime between '2019-01-15 00:00:00' and '2019-01-15 23:59:59'

# Question 4. Largest trip for each day
select
  date(lpep_pickup_datetime) as d,
  max(trip_distance) as total_distance
from green_2019_hw gh 
where lpep_pickup_datetime between '2019-01-01 00:00:00' and '2019-01-31 23:59:59'
group by 1
order by total_distance desc
limit 1

# Question 5. The number of passengers
select
  sum(case when passenger_count=2 then 1 else 0 end) as two_pass,
  sum(case when passenger_count=3 then 1 else 0 end) as three_pass
from green_2019_hw gh 
where lpep_pickup_datetime between '2019-01-01 00:00:00' and '2019-01-01 23:59:59'


# Question 6. Largest tip
select
  z2."Zone",
  max(gh.tip_amount) as tip
from green_2019_hw gh 
  left join zones z1 on z1."LocationID" = gh."PULocationID" 
  left join zones z2 on z2."LocationID" = gh."DOLocationID" 
where lpep_pickup_datetime between '2019-01-01 00:00:00' and '2019-01-31 23:59:59'
  and z1."Zone" = 'Astoria'
group by 1
order by tip desc 
limit 1
