import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df.drop_duplicates().reset_index(drop = True)
    df['Id'] = df.index
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    # Vendor_Dim = pd.DataFrame.from_dict({'Vendor_Name':['Creative Mobile Technologies, LLC','VeriFone Inc']})
    Vendor_Dict = {1:'Creative Mobile Technologies, LLC',2:'VeriFone Inc'}
    Vendor_Dim = df[['VendorID']].drop_duplicates().reset_index(drop = True)
    # Vendor_Dim['Id'] = Vendor_Dim['VendorID'].index
    Vendor_Dim['Vendor_Name'] = Vendor_Dim['VendorID'].map(Vendor_Dict)
    Vendor_Dim = Vendor_Dim[['VendorID','Vendor_Name']]
    RateCode_Dict = {1:'Standard rate',2:'JFK',3:'Newark',4:'Nassau or Westchester',5:'Negotiated fare',6:'Group ride'}
    RateCode_Dim = df[['RatecodeID']].drop_duplicates().reset_index (drop = True).sort_values(by = 'RatecodeID')
    RateCode_Dim['RateCode'] = RateCode_Dim['RatecodeID'].map(RateCode_Dict)
    StoreFwd_Dim = pd.DataFrame.from_dict({'StoreFwd_Id':[1,2],'Flag':['Y','N']})
    Payment_Dict = {1:'Credit card',2:'Cash',3:'No charge',4:'Dispute',5:'Unknown',6:'Voided trip'}
    PaymentType_Dim = df[['payment_type']].drop_duplicates().reset_index(drop = True)
    PaymentType_Dim.rename(columns = {'payment_type':'Payment_Type_Id'},inplace = True)
    PaymentType_Dim['Payment_Type'] = PaymentType_Dim['Payment_Type_Id'].map(Payment_Dict)
    PickUp_Location_Dim = df[['pickup_longitude',	'pickup_latitude']].reset_index(drop = True)
    PickUp_Location_Dim['PickupLoc_Id'] = PickUp_Location_Dim.index
    PickUp_Location_Dim = PickUp_Location_Dim[['PickupLoc_Id','pickup_longitude','pickup_latitude']]
    Dropoff_Location_Dim = df[['dropoff_longitude',	'dropoff_latitude']].reset_index(drop = True)
    Dropoff_Location_Dim['DropoffLoc_Id'] = Dropoff_Location_Dim.index
    Dropoff_Location_Dim = Dropoff_Location_Dim[['DropoffLoc_Id','dropoff_longitude',	'dropoff_latitude']]
    Pickup_Time_Dim = df[['tpep_pickup_datetime']].reset_index(drop = True)
    Pickup_Time_Dim['Pickup_Date_Id'] = Pickup_Time_Dim.index
    Pickup_Time_Dim = Pickup_Time_Dim[['Pickup_Date_Id','tpep_pickup_datetime']]
    Pickup_Time_Dim['Pick_Date'] = Pickup_Time_Dim['tpep_pickup_datetime'].dt.date
    Pickup_Time_Dim['Pick_Month'] = Pickup_Time_Dim['tpep_pickup_datetime'].dt.month
    Pickup_Time_Dim['Pick_Year'] = Pickup_Time_Dim['tpep_pickup_datetime'].dt.year
    Pickup_Time_Dim['Pick_Day'] = Pickup_Time_Dim['tpep_pickup_datetime'].dt.day
    Pickup_Time_Dim['Pick_Month_Name'] = Pickup_Time_Dim['tpep_pickup_datetime'].dt.month_name()
    Pickup_Time_Dim['Pick_Day_Name'] = Pickup_Time_Dim['tpep_pickup_datetime'].dt.day_name()
    Pickup_Time_Dim = Pickup_Time_Dim[['Pickup_Date_Id'	,'tpep_pickup_datetime',	'Pick_Date',	'Pick_Year','Pick_Month',	'Pick_Day',	'Pick_Month_Name',	'Pick_Day_Name']].rename(columns = {'tpep_pickup_datetime':'Pickup_DateTime'})
    Drop_Time_Dim = df[['tpep_dropoff_datetime']].reset_index(drop = True)
    Drop_Time_Dim['Dropoff_Date_Id'] = Drop_Time_Dim.index
    Drop_Time_Dim = Drop_Time_Dim[['Dropoff_Date_Id','tpep_dropoff_datetime']].rename(columns = {'tpep_dropoff_datetime':'Dropoff_DateTime'})
    Drop_Time_Dim['Drop_Date'] = Drop_Time_Dim['Dropoff_DateTime'].dt.date
    Drop_Time_Dim['Drop_Date'] = pd.to_datetime(Drop_Time_Dim['Drop_Date'])
    Drop_Time_Dim['Drop_Year'] = Drop_Time_Dim['Drop_Date'].dt.year
    Drop_Time_Dim['Drop_Month'] = Drop_Time_Dim['Drop_Date'].dt.month
    Drop_Time_Dim['Drop_Day'] = Drop_Time_Dim['Drop_Date'].dt.day
    Drop_Time_Dim['Drop_Month_Name'] = Drop_Time_Dim['Drop_Date'].dt.month_name()
    Drop_Time_Dim['Drop_Day_Name'] = Drop_Time_Dim['Drop_Date'].dt.day_name()
    fact_df = df.merge(Vendor_Dim,on = 'VendorID',how = 'left').merge(RateCode_Dim,on = 'RatecodeID',how = 'left').merge(StoreFwd_Dim,left_on = 'store_and_fwd_flag',right_on = 'Flag',how = 'left').merge(PaymentType_Dim,left_on = 'payment_type',right_on = 'Payment_Type_Id',how = 'left').merge(PickUp_Location_Dim,left_on = ['Id'],right_on = ['PickupLoc_Id'],how = 'left').merge(Dropoff_Location_Dim,left_on = ['Id'],right_on = ['DropoffLoc_Id'],how = 'left').merge(Pickup_Time_Dim,left_on = 'Id',right_on = 'Pickup_Date_Id',how = 'left').merge(Drop_Time_Dim,left_on = 'Id',right_on = 'Dropoff_Date_Id',how = 'left')
    fact_df = fact_df[['Id','VendorID','Pickup_Date_Id','Dropoff_Date_Id','passenger_count','trip_distance','PickupLoc_Id','RatecodeID','StoreFwd_Id','DropoffLoc_Id','Payment_Type_Id','fare_amount','extra','mta_tax','tip_amount','tolls_amount','tolls_amount','improvement_surcharge','total_amount']]
    # print(fact_df)
    return {'Vendor_Dim':Vendor_Dim.to_dict(orient = 'dict'),
            'RateCode_Dim':RateCode_Dim.to_dict(orient = 'dict'),
            'StoreFwd_Dim':StoreFwd_Dim.to_dict(orient = 'dict'),
            'PaymentType_Dim':PaymentType_Dim.to_dict(orient = 'dict'),
            'PickUp_Location_Dim':PickUp_Location_Dim.to_dict(orient = 'dict'),
            'Dropoff_Location_Dim':Dropoff_Location_Dim.to_dict(orient = 'dict'),
            'Pickup_Time_Dim':Pickup_Time_Dim.to_dict(orient = 'dict'),
            'Drop_Time_Dim':Drop_Time_Dim.to_dict(orient = 'dict'),
            'fact_df':fact_df.to_dict(orient = 'dict')}




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
