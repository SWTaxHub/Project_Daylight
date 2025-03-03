import pandas as pd
import os

# Define file paths
timesheet_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_cas_filtered_rules.parquet'
output_test_sample = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\deptid_sample.xlsx'

# Load the Parquet file
timesheet_df = pd.read_parquet(timesheet_file_path)

# Step 1: Filter rows where DEPTID is either 5 or 44
deptid_filtered_df = timesheet_df[
    (timesheet_df['DEPTID'].isin(['5', '44'])) &
    (timesheet_df['> 22/11/2023 Span of Hours'] != 'nan')  # Ensure the field is not the string 'nan'
]
# Step 2: Save the filtered sample to an Excel file
deptid_filtered_df.head(20000).to_excel(output_test_sample, index=False)
print(f"Sample with DEPTID 5 or 44 saved to: {output_test_sample}")
