import pandas as pd
import os

# Step 1: Define paths for files and output directories
parquet_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_daily_weekly.parquet'
output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'

# Step 2: Load the Parquet file
timesheet_data = pd.read_parquet(parquet_file_path)

# Step 3: Filter transactions where 'Rule - Weekend Penalty' is 'y'
weekend_penalty_transactions = timesheet_data[timesheet_data['Rule - Weekend Penalty'] == 'y']

# Step 4: Extract distinct 'Grade-Step OR Course Code' values from filtered data
distinct_grade_step = weekend_penalty_transactions[['Grade-Step OR Course Code']].drop_duplicates()

# Step 5: Output the filtered transactions and distinct list to an Excel file
output_file = os.path.join(output_tests, 'weekend_penalty_transactions_and_distinct_grades.xlsx')

# Save both dataframes to separate sheets in the same Excel file
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    weekend_penalty_transactions.to_excel(writer, sheet_name='Weekend Penalty Transactions', index=False)
    distinct_grade_step.to_excel(writer, sheet_name='Distinct Grade-Step Codes', index=False)

print(f"Test results saved to {output_file}")

