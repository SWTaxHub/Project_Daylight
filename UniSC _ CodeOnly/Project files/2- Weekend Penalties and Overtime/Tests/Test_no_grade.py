import pandas as pd
import os

# Define file paths and directories
output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
timesheet_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_cas_filtered_rules.parquet'

# Ensure the output directory exists
if not os.path.exists(output_tests):
    os.makedirs(output_tests)

# Load the timesheet data
timesheet_cas_filtered_rules = pd.read_parquet(timesheet_file_path)

# Step 1: Extract distinct list of 'Grade-Step OR Course Code'
distinct_grade_step = timesheet_cas_filtered_rules['Grade-Step OR Course Code'].dropna().unique()

# Step 2: Convert the list into a DataFrame for output
distinct_grade_step_df = pd.DataFrame(distinct_grade_step, columns=['Distinct Grade-Step OR Course Code'])

# Step 3: Filter transactions where 'Grade-Step OR Course Code' is either null or "/0"
null_or_0_transactions = timesheet_cas_filtered_rules[
    (timesheet_cas_filtered_rules['Grade-Step OR Course Code'].isna()) |  # Check for nulls
    (timesheet_cas_filtered_rules['Grade-Step OR Course Code'] == '/0') |  # Check for "/0"
    (timesheet_cas_filtered_rules['Grade-Step OR Course Code'].str.strip() == '')  # Check for empty strings
]


# Step 4: Output both data to different sheets in the same Excel file
output_file = os.path.join(output_tests, 'distinct_grade_step_or_course_code.xlsx')

# Using ExcelWriter to write to multiple sheets
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    # Sheet 1: Distinct list of Grade-Step OR Course Code
    distinct_grade_step_df.to_excel(writer, sheet_name='Distinct_Grade_Step', index=False)

    # Sheet 2: Transactions with Grade-Step OR Course Code == null or '/0'
    null_or_0_transactions.to_excel(writer, sheet_name='Null_or_0_Transactions', index=False)

# Print confirmation
print(f"Distinct list and transactions saved to {output_file}")
