import pandas as pd
import numpy as np
import os
from glob import glob

DATA_DIR = './taxi data/'

file_paths = glob(os.path.join(DATA_DIR, '*.parquet'))

print(f"Found {len(file_paths)} files to read.")
try:
    df_list = [pd.read_parquet(file_path, engine='pyarrow') for file_path in file_paths]
    df_raw_combined = pd.concat(df_list, ignore_index=True)
    print(f"Successfully combined files into a master DataFrame with shape: {df_raw_combined.shape}")
except Exception as e:
    print(f"Error loading file {e}")

#15 minute bin BIN_SIZE
BIN_SIZE = '15T' 

#create the Time_bin column by rounding the pickup time down
df_raw['Time_Bin'] = df_raw_combined['tpep_pickup_datetime'].dt.floor(BIN_SIZE)

#calculate demand (active requests)
#demand is defined by trips originating in a zone 

demand_df = (df_raw_combined.groupby(['Time_Bin', 'PULocationID'])
             .size().rename('ActiveRequests').reset_index())

#Calculate supply Proxy (Available Drivers arriving)
#supply is proxied by the flow of drivers arriving at a zone (Dropoff Location)
supply_df = (df_raw_combined.groupby(['Time_Bin', 'DOLocationID'])
             .size().rename('AvailableDriversProxy').reset_index())

supply_df = supply_df.rename(columns={'DOLocationID': 'PULocationID'})

#use a full outer merge to ensure we capture all zone-time comninations 
df_appregate = pd.merge(
        demand_df,
        supply_df,
        on=['Time_Bin','PULocationID'],
        how='outer'
    )

#clean and calculate the surge proxy (demand excess ratio)
#fill NaN Values with 0

df_aggregate[['ActiveRequests', 'AvailableDriversProxy']] = df_aggregate[
        ['ActiveRequests', 'AvailableDriversProxy']].fillna(0)

df_appregate['SupplyElasticity'] = np.divide(
        df_appregate['AvailableDriversProxy'],
        df_appregate['ActiveRequests'],
        out=np.zeros_like(df_appregate['ActiveRequests'], dtype=float), #this handles division by 0
        where=df_appregate['ActiveRequests'] != 0) #make sure denominator is not 0

#target variable y for prediction: Demand Excess Ratio
df_appregate['DER_t'] = np.divide(
        df_appregate['ActiveRequests'],
        df_appregate['AvailableDriversProxy'],
        out=np.full_like(df_appregate['ActiveRequests'], fill_value=1, dtype=float), #default to 1
        where=df_appregate['AvailableDriversProxy'] != 0)
