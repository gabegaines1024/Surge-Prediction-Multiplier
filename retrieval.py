import pandas as pd
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

demand_df = (df_raw_combined.groupby(['Time_Bin', 'PULocationID']).size().rename('ActiveRequests').reset_index())

#Calculate supply Proxy (Available Drivers arriving)
#supply is proxied by the flow of drivers arriving at a zone (Dropoff Location)
supply_df = (df_raw_combined.groupby(['Time_Bin', 'DOLocationID']).size().rename('AvailableDriversProxy').reset_index())
