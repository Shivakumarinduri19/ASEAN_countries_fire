import pandas as pd

# 1. Load and Pre-process
df = pd.read_csv('/content/fire_archive_719030_vietnam.csv')
df['acq_date'] = pd.to_datetime(df['acq_date'])

# Filter: Confidence > 80 and Year 2005-2025
df_filtered = df[
    (df['confidence'] > 80) &
    (df['acq_date'].dt.year >= 2005) &
    (df['acq_date'].dt.year <= 2025)
].copy()

# Extract Year and Month
df_filtered['Year'] = df_filtered['acq_date'].dt.year
df_filtered['Month'] = df_filtered['acq_date'].dt.month

# 2. Perform All Calculations in One GroupBy
# This calculates Count, Mean, and Median at once
final_results = df_filtered.groupby(['Year', 'Month']).agg(
    total_fire_count=('acq_date', 'count'),
    frp_mean=('frp', 'mean'),
    frp_median=('frp', 'median')
).reset_index()

# 3. Export to a Single CSV
final_results.to_csv('Fire_Analysis_Final_2005_2025.csv', index=False)

print("Combined CSV generated successfully.")
