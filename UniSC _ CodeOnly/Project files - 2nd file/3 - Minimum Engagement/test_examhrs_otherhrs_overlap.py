import pandas as pd
import os

# Define file paths
#timesheet_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet'
timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_with_student_and_perm_indicator.parquet"

#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"

# Load the timesheet data
timesheet_min_top_up_cals = pd.read_parquet(timesheet_path)

# Step 1: Create a flag for rows where POSITION_NBR is '1085'
timesheet_min_top_up_cals['is_position_1085'] = timesheet_min_top_up_cals['POSITION_NBR'] == '1085'

# Step 2: Group by EMPLID and date_only and check if both '1085' and other positions exist on the same day
# Group by EMPLID and date_only, then check if both '1085' and other positions are present
mixed_position_days = timesheet_min_top_up_cals.groupby(['EMPLID', 'date_only']).filter(
    lambda group: group['is_position_1085'].any() and not group['is_position_1085'].all()
)

# Step 3: Output the filtered transactions to an Excel file
mixed_position_output_file = os.path.join(output_tests, 'mixed_position_days_sample.xlsx')
mixed_position_days.to_excel(mixed_position_output_file, index=False)

print(f"Sample of mixed position days saved to: {mixed_position_output_file}")
