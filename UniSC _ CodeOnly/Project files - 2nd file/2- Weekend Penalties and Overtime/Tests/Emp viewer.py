import pandas as pd

# Step 1: Define the paths for the Parquet files
timesheet_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_daily_weekly.parquet'
original_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_include_SAL_ADMIN_PLAN.parquet'

# Step 2: Set the filter variables
empid = '1002303'  # Ensure EMPLID is treated as a string
month = 2  # February
year = 2019

# Step 3: Load the Parquet files
timesheet_data = pd.read_parquet(timesheet_file_path)
original_data = pd.read_parquet(original_file_path)

# Ensure that 'DATE WORKED' is in datetime format
timesheet_data['DATE WORKED'] = pd.to_datetime(timesheet_data['DATE WORKED'])
original_data['DATE WORKED'] = pd.to_datetime(original_data['DATE WORKED'])

# Ensure that 'EMPLID' is treated as a string in both datasets
timesheet_data['EMPLID'] = timesheet_data['EMPLID'].astype(str)
original_data['EMPLID'] = original_data['EMPLID'].astype(str)

# Step 4: Filter for the specified EMPLID, month, and year in both datasets
filtered_timesheet_data = timesheet_data[
    (timesheet_data['EMPLID'] == empid) &
    (timesheet_data['DATE WORKED'].dt.month == month) &
    (timesheet_data['DATE WORKED'].dt.year == year)
]

filtered_original_data = original_data[
    (original_data['EMPLID'] == empid) &
    (original_data['DATE WORKED'].dt.month == month) &
    (original_data['DATE WORKED'].dt.year == year)
]

# Step 5: Generate a dynamic output file name
output_file = f'C:\\Users\\zhump\\Documents\\Data Analytics\\Project Daylight\\Outputs\\Tests\\emplid_{empid}_month_{month}_year_{year}.xlsx'

# Step 6: Save both filtered datasets to different tabs in the same Excel file
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    filtered_timesheet_data.to_excel(writer, sheet_name='Filtered Timesheet Data', index=False)
    filtered_original_data.to_excel(writer, sheet_name='Filtered Original Data', index=False)

print(f"Filtered data saved to {output_file}")
