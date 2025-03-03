import pandas as pd

# Define file path
timesheet_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_cas_filtered_rules.parquet'

# Load the Parquet file
timesheet_df = pd.read_parquet(timesheet_file_path)

# Check if 'DATE WORKED' is in datetime format; if not, convert it
if not pd.api.types.is_datetime64_any_dtype(timesheet_df['DATE WORKED']):
    timesheet_df['DATE WORKED'] = pd.to_datetime(timesheet_df['DATE WORKED'])

# Calculate the minimum and maximum date worked
min_date_worked = timesheet_df['DATE WORKED'].min()
max_date_worked = timesheet_df['DATE WORKED'].max()

# Output the result
print(f"Minimum 'DATE WORKED': {min_date_worked}")
print(f"Maximum 'DATE WORKED': {max_date_worked}")
