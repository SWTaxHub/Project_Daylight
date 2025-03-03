import pandas as pd
import os

# Define file paths
timesheet_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\cleaned_combined_timesheet_data.parquet'
output_test_sample = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\deptid_department_position_sample.xlsx'

# Load the Parquet file
timesheet_df = pd.read_parquet(timesheet_file_path)

# Step 1: Ensure DEPTID is treated as a string
timesheet_df['DEPTID'] = timesheet_df['DEPTID'].astype(str)

# Step 2: Filter for the specific conditions
filtered_df = timesheet_df[
    (timesheet_df['DEPTID'] == '5') &
    (timesheet_df['Department Name'] == 'USC Clinical Trials Centre') &
    (timesheet_df['Position Title'] == 'Clinical Trials Ops Man - Act')
]

# Step 3: Save the filtered sample to an Excel file
filtered_df.to_excel(output_test_sample, index=False)
print(f"Filtered sample saved to: {output_test_sample}")
