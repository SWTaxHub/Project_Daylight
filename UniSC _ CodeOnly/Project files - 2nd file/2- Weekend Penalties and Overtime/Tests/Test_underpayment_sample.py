import pandas as pd
import os
import numpy as np
import datetime

# Step 1: Define paths for files and output directories
timesheet_cas_OT_daily_weekly_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_daily_weekly.parquet'
output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'

# Step 2: Load the Parquet file
timesheet_cas_OT_daily_weekly = pd.read_parquet(timesheet_cas_OT_daily_weekly_path)


# Step 3: Find all unique EMPID_week_id where any of the specified conditions are met
condition = (
    (timesheet_cas_OT_daily_weekly['cal_weekly_ot_hours'] > 0) |
    (timesheet_cas_OT_daily_weekly['cal_daily_ot_hours'] > 0) |
    (timesheet_cas_OT_daily_weekly['cal_ot_span_as_hours'] > 0) |
    (timesheet_cas_OT_daily_weekly['cal_ot_span_bs_hours'] > 0) |
    (timesheet_cas_OT_daily_weekly['cal_ot_span_weekend_hours'] > 0)
)

# Filter for unique EMPID_week_id where conditions are met
empid_week_id_with_overtime = timesheet_cas_OT_daily_weekly.loc[condition, 'EMPLID_week_id'].unique()

# Step 6: Pull all transactions matching the EMPID_week_id with overtime
matching_transactions = timesheet_cas_OT_daily_weekly[timesheet_cas_OT_daily_weekly['EMPLID_week_id'].isin(empid_week_id_with_overtime)]

# Step 7: Output the matching transactions to Excel as a sample
current_date = datetime.datetime.now().strftime('%Y-%m-%d')  # Get current date in 'YYYY-MM-DD' format
output_file = os.path.join(output_tests, f'underpayment_sample_transactions_{current_date}.xlsx')  # Append the date to the file name

matching_transactions.to_excel(output_file, index=False)

print(f"Sample of matching transactions saved to {output_file}")
