CREATE TABLE uber-data-pipeline-427415.Uber_DataWarehouse.Uber_Data_Analytics AS(
select v.vendor_name,
pt.*,
dt.*,
pl.pickup_longitude,
pl.pickup_latitude,
rc.RateCode,
sf.Flag,
dl.dropoff_longitude,
dl.dropoff_latitude,
ptd.Payment_Type,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
from uber-data-pipeline-427415.Uber_DataWarehouse.fact_df f
join uber-data-pipeline-427415.Uber_DataWarehouse.Vendor_Dim v
on v.VendorID = f.VendorID
join uber-data-pipeline-427415.Uber_DataWarehouse.Pickup_Time_Dim pt
on pt.Pickup_Date_Id = f.Pickup_Date_Id
join uber-data-pipeline-427415.Uber_DataWarehouse.Drop_Time_Dim dt
on dt.Dropoff_Date_Id = f.Dropoff_Date_Id
join uber-data-pipeline-427415.Uber_DataWarehouse.PickUp_Location_Dim pl
on pl.PickupLoc_Id = f.PickupLoc_Id
join uber-data-pipeline-427415.Uber_DataWarehouse.RateCode_Dim rc
on rc.RatecodeID = f.RatecodeID
join uber-data-pipeline-427415.Uber_DataWarehouse.StoreFwd_Dim sf
on sf.StoreFwd_Id = f.StoreFwd_Id
join uber-data-pipeline-427415.Uber_DataWarehouse.Dropoff_Location_Dim dl
on dl.DropoffLoc_Id = f.DropoffLoc_Id
join uber-data-pipeline-427415.Uber_DataWarehouse.PaymentType_Dim ptd
on ptd.Payment_Type_Id = f.Payment_Type_Id
);

ALTER TABLE uber-data-pipeline-427415.Uber_DataWarehouse.Uber_Data_Analytics
DROP COLUMN dropoff_date_id