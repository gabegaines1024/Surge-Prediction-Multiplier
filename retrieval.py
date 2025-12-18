import pandas as pd
import numpy as np
import os
from glob import glob
import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

# Configure Dask for memory efficiency
dask.config.set({
    'dataframe.shuffle.method': 'tasks',  # More memory efficient for groupby operations
    'distributed.worker.memory.target': 0.75,  # Start spilling to disk at 75% RAM
    'distributed.worker.memory.spill': 0.85,   # Aggressive spilling at 85%
    'distributed.worker.memory.pause': 0.95,   # Pause at 95%
})

DATA_DIR = './taxi data/'
OUTPUT_DIR = './processed_data/'  # Where we'll save processed chunks
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Separate file paths by type (they have different column schemas!)
yellow_files = glob(os.path.join(DATA_DIR, 'yellow_tripdata_*.parquet'))
fhvhv_files = glob(os.path.join(DATA_DIR, 'fhvhv_tripdata_*.parquet'))

print(f"Found {len(yellow_files)} yellow taxi files")
print(f"Found {len(fhvhv_files)} FHVHV (Uber/Lyft) files")
print("=" * 60)
print("DASK MODE: Loading data lazily (no memory consumed yet)...")
print("=" * 60)

# Function to standardize column names across different taxi data formats
def standardize_columns(df, source_type):
    """Rename columns to standard names and select only needed columns."""
    if source_type == 'yellow':
        # Yellow taxi columns
        df = df.rename(columns={
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime'
        })
    # FHVHV already uses 'pickup_datetime' and 'dropoff_datetime'
    
    # Select only the columns we need (common to both)
    required_cols = ['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID']
    return df[required_cols]

# Load and standardize each data source
dfs = []

if yellow_files:
    print("Loading yellow taxi data...")
    df_yellow = dd.read_parquet(yellow_files, engine='pyarrow')
    df_yellow = standardize_columns(df_yellow, 'yellow')
    dfs.append(df_yellow)
    print(f"  → Yellow: {df_yellow.npartitions} partitions")

if fhvhv_files:
    print("Loading FHVHV (Uber/Lyft) data...")
    df_fhvhv = dd.read_parquet(fhvhv_files, engine='pyarrow')
    df_fhvhv = standardize_columns(df_fhvhv, 'fhvhv')
    dfs.append(df_fhvhv)
    print(f"  → FHVHV: {df_fhvhv.npartitions} partitions")

# Combine all data sources
if len(dfs) > 1:
    df_raw_combined = dd.concat(dfs, interleave_partitions=True)
elif len(dfs) == 1:
    df_raw_combined = dfs[0]
else:
    raise ValueError("No parquet files found in the data directory!")

print(f"\nCombined DataFrame: {df_raw_combined.npartitions} partitions")
print("No data loaded into RAM yet - operations will execute on-demand.")

# ============================================================================
# STEP 1: TIME BINNING & AGGREGATIONS (Still Lazy!)
# ============================================================================
BIN_SIZE = '15min'  # 15-minute bins

print("\n[1/6] Creating time bins...")
# Create the Time_Bin column by rounding the pickup time down
# (using standardized column name 'pickup_datetime')
df_raw_combined['Time_Bin'] = df_raw_combined['pickup_datetime'].dt.floor(BIN_SIZE)

print("[2/6] Computing demand aggregation (lazy)...")
# Calculate demand (active requests) - trips originating in a zone
demand_df = (df_raw_combined.groupby(['Time_Bin', 'PULocationID'])
             .size()
             .to_frame(name='ActiveRequests')
             .reset_index())

print("[3/6] Computing supply aggregation (lazy)...")
# Calculate supply proxy (available drivers arriving at a zone via dropoff)
supply_df = (df_raw_combined.groupby(['Time_Bin', 'DOLocationID'])
             .size()
             .to_frame(name='AvailableDriversProxy')
             .reset_index())

supply_df = supply_df.rename(columns={'DOLocationID': 'PULocationID'})

print("[4/6] Merging demand and supply (lazy)...")
# Use a full outer merge to capture all zone-time combinations
# NOTE: Dask merge can be memory-intensive, so we'll compute this strategically
df_aggregate = dd.merge(
        demand_df,
        supply_df,
        on=['Time_Bin', 'PULocationID'],
        how='outer'
    )

# ============================================================================
# STEP 2: CALCULATE DERIVED METRICS (Still Lazy!)
# ============================================================================
print("[5/6] Calculating derived metrics (DER, Supply Elasticity)...")

# Fill NaN values with 0
df_aggregate['ActiveRequests'] = df_aggregate['ActiveRequests'].fillna(0)
df_aggregate['AvailableDriversProxy'] = df_aggregate['AvailableDriversProxy'].fillna(0)

# Supply Elasticity: AvailableDrivers / ActiveRequests
# Use Dask-compatible division with safe handling of division by zero
df_aggregate['SupplyElasticity'] = (
    df_aggregate['AvailableDriversProxy'] / 
    df_aggregate['ActiveRequests'].replace(0, np.nan)
).fillna(0)

# Target variable: Demand Excess Ratio (DER_t)
# DER = ActiveRequests / AvailableDrivers
df_aggregate['DER_t'] = (
    df_aggregate['ActiveRequests'] / 
    df_aggregate['AvailableDriversProxy'].replace(0, np.nan)
).fillna(1.0)  # Default to 1 when no drivers available

# Rename for clarity
df_aggregate = df_aggregate.rename(columns={'PULocationID': 'Zone'})


# ============================================================================
# STEP 3: MATERIALIZE AGGREGATED DATA (First Compute!)
# ============================================================================
# The aggregated data is MUCH smaller than 80M rows (Zone x 15min bins)
# We can safely compute this into memory or write to disk
print("\n" + "=" * 60)
print("COMPUTING AGGREGATIONS... This will take several minutes.")
print("Progress bar will show below:")
print("=" * 60)

# Persist intermediate result to parquet for checkpoint (optional but recommended)
print("\nWriting aggregated data to disk (checkpoint)...")
df_aggregate.to_parquet(
    os.path.join(OUTPUT_DIR, 'aggregated_temp.parquet'),
    engine='pyarrow',
    compression='snappy'
)
print("✓ Aggregated data saved to disk.")

# Read back the aggregated data (still Dask, but now from a single source)
df_aggregate = dd.read_parquet(
    os.path.join(OUTPUT_DIR, 'aggregated_temp.parquet'),
    engine='pyarrow'
)

# ============================================================================
# STEP 4: TIME-SERIES FEATURE ENGINEERING WITH DASK
# ============================================================================
print("\n[6/6] Creating time-series features (lags, shifts, velocity)...")

# For time-series operations like shift, we need to use map_partitions
# with a custom function that ensures proper grouping within partitions
def create_time_series_features(df):
    """
    Custom function to create lagged features within each partition.
    This ensures groupby operations respect zone boundaries.
    """
    # Ensure data is sorted within partition
    df = df.sort_values(by=['Zone', 'Time_Bin'])
    
    # Shift the DER forward by one period (15 minutes) to create TARGET variable
    df['Target_DER_t+15'] = df.groupby('Zone')['DER_t'].shift(-1)
    
    # Create Lagged DER features (Lagged Features: t-15 and t-30)
    df['Lag_DER_t-15'] = df.groupby('Zone')['DER_t'].shift(1)  # Shift 1 period backward
    df['Lag_DER_t-30'] = df.groupby('Zone')['DER_t'].shift(2)  # Shift 2 periods backward
    
    # Calculate current Demand Velocity: Requests(t) - Requests(t-15)
    df['DemandVelocity_t'] = df.groupby('Zone')['ActiveRequests'].diff(periods=1)
    
    # Lag the calculated Demand Velocity by one period
    df['Lag_DemandVelocity_t-15'] = df.groupby('Zone')['DemandVelocity_t'].shift(1)
    
    return df

# CRITICAL: For shift operations to work correctly across partitions,
# we need to repartition by Zone to ensure each zone's data stays together
print("Repartitioning by Zone for time-series operations...")

# Repartition by Zone (this ensures all time-series for a zone are in same partition)
df_aggregate = df_aggregate.set_index('Zone')

# Apply the time-series feature function with map_partitions
print("Applying time-series transformations...")
df_aggregate = df_aggregate.map_partitions(
    create_time_series_features,
    meta={
        'Time_Bin': 'datetime64[ns]',
        'ActiveRequests': 'float64',
        'AvailableDriversProxy': 'float64',
        'SupplyElasticity': 'float64',
        'DER_t': 'float64',
        'Target_DER_t+15': 'float64',
        'Lag_DER_t-15': 'float64',
        'Lag_DER_t-30': 'float64',
        'DemandVelocity_t': 'float64',
        'Lag_DemandVelocity_t-15': 'float64'
    }
)

# Reset index to make Zone a regular column again
df_aggregate = df_aggregate.reset_index()

# ============================================================================
# STEP 5: TRAIN/TEST SPLIT & PERSISTENCE
# ============================================================================
print("\n" + "=" * 60)
print("SPLITTING DATA AND WRITING TO DISK...")
print("=" * 60)

# Define the split date (data is from Jan-Mar 2025, so split at Feb 15)
SPLIT_DATE = pd.to_datetime('2025-02-15')

# Separate the data using Dask filtering
df_train = df_aggregate[df_aggregate['Time_Bin'] < SPLIT_DATE]
df_test = df_aggregate[df_aggregate['Time_Bin'] >= SPLIT_DATE]

# Handle NaNs: Drop rows with NaN in the target
# These are the last 15-min rows in each zone that have features but no future target (t+15)
df_train = df_train.dropna(subset=['Target_DER_t+15'])
df_test = df_test.dropna(subset=['Target_DER_t+15'])

# Define features and target
# We'll compute column names from a small sample
sample_cols = df_train.head(1).columns.tolist()
FEATURES = [col for col in sample_cols if 'Lag' in col or 'Elasticity' in col or 'Velocity' in col]
TARGET = 'Target_DER_t+15'

print(f"\nFeatures identified: {FEATURES}")
print(f"Target: {TARGET}")

# ============================================================================
# STEP 6: WRITE PROCESSED DATA TO DISK (Final Compute!)
# ============================================================================
print("\n" + "=" * 60)
print("FINAL COMPUTATION: Writing train/test datasets to parquet...")
print("This is where Dask executes the entire pipeline.")
print("=" * 60)

# Write train data
print("\n[1/2] Computing and writing TRAIN dataset...")
with ProgressBar():
    df_train.to_parquet(
        os.path.join(OUTPUT_DIR, 'train_data.parquet'),
        engine='pyarrow',
        compression='snappy'
    )
print(f"✓ Train data saved to: {OUTPUT_DIR}/train_data.parquet")

# Write test data
print("\n[2/2] Computing and writing TEST dataset...")
with ProgressBar():
    df_test.to_parquet(
        os.path.join(OUTPUT_DIR, 'test_data.parquet'),
        engine='pyarrow',
        compression='snappy'
    )
print(f"✓ Test data saved to: {OUTPUT_DIR}/test_data.parquet")

# ============================================================================
# STEP 7: LOAD PROCESSED DATA FOR MODEL TRAINING (Optional)
# ============================================================================
print("\n" + "=" * 60)
print("LOADING PROCESSED DATA FOR MODEL TRAINING")
print("=" * 60)

# Now you can load just the features you need into memory
# This will be MUCH smaller than the original 80M rows
print("\nReading processed train data...")
train_df_processed = pd.read_parquet(os.path.join(OUTPUT_DIR, 'train_data.parquet'))
print(f"Train shape: {train_df_processed.shape}")

print("Reading processed test data...")
test_df_processed = pd.read_parquet(os.path.join(OUTPUT_DIR, 'test_data.parquet'))
print(f"Test shape: {test_df_processed.shape}")

# Separate features and target
X_train, y_train = train_df_processed[FEATURES], train_df_processed[TARGET]
X_test, y_test = test_df_processed[FEATURES], test_df_processed[TARGET]

print(f"\nFinal Dataset Summary:")
print(f"X_train shape: {X_train.shape}")
print(f"X_test shape: {X_test.shape}")
print(f"\n✓ Data pipeline complete! Ready for model training.")
print("=" * 60)
