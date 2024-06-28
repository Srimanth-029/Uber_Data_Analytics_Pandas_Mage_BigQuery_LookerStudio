
--based on number of trips select the top 10 pickup locations of uber
select pd.pickup_longitude,pd.pickup_latitude,count(*) as count from `uber-data-pipeline-427415.Uber_DataWarehouse.fact_df` f
join uber-data-pipeline-427415.Uber_DataWarehouse.PickUp_Location_Dim pd
on f.PickupLoc_Id = pd.PickupLoc_Id
group by pickup_longitude,pickup_latitude
order by count desc
limit 10;

--total number of trips by passenger count
select passenger_count,count(*) from uber-data-pipeline-427415.Uber_DataWarehouse.fact_df f
group by passenger_count
order by passenger_count;

--average fare amount by hour of the day
select extract(hour from datetime(pt.Pickup_DateTime)) as hour,avg(fare_amount) from uber-data-pipeline-427415.Uber_DataWarehouse.fact_df f
join uber-data-pipeline-427415.Uber_DataWarehouse.Pickup_Time_Dim pt
on pt.Pickup_Date_Id = f.Pickup_Date_Id
group by hour
order by hour desc;