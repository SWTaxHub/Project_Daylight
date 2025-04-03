import pandas as pd

# Define input and output paths
#output_directory = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data'
output_directory = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"

timesheet_with_student_indicator_path = output_directory + r'\timesheet_with_student_indicator.parquet'
summary_hr_data_permanent_path = output_directory + r'\summary_hr_data_permanent.parquet'
#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"
# Load the required Parquet files
timesheet_df = pd.read_parquet(timesheet_with_student_indicator_path)
summary_hr_df = pd.read_parquet(summary_hr_data_permanent_path)

# Step 1: Store the original number of rows before any modifications (for testing later)
original_row_count = len(timesheet_df)

# Step 2: Filter for relevant columns (index, EMPLID, DATE WORKED)
timesheet_df_filtered = timesheet_df[['index', 'EMPLID', 'DATE WORKED']].copy()

print('timesheet df filtered columns: ')
print(timesheet_df_filtered.columns)

timesheet_df_filtered.to_csv('timesheet_df_filtered.csv')


# added 30/01/24 due to run error where columns were different

# Rename the column 'old_column_name' to 'new_column_name'
#summary_hr_df.rename(columns={'EMPID_EMPL_RCD': 'EMPLID'}, inplace=True)

# Shorten the 'column_name' to 7 characters (left to right)
summary_hr_df['EMPLID'] = summary_hr_df['EMPLID'].str[:7]
print('summaryHR columns: ')
print(summary_hr_df.columns)




print('summary Hr csv printed: ')
summary_hr_df.to_csv('summary_hr_df.csv')

# Step 3: Convert `DATE WORKED`, `earliest_date`, and `latest_date` to datetime if not already
timesheet_df_filtered['DATE WORKED'] = pd.to_datetime(timesheet_df_filtered['DATE WORKED'])
summary_hr_df['earliest_date'] = pd.to_datetime(summary_hr_df['earliest_date'])
summary_hr_df['latest_date'] = pd.to_datetime(summary_hr_df['latest_date'])

# Step 4: Perform a left join between the filtered timesheet and the HR data based on EMPLID
merged_df = pd.merge(timesheet_df_filtered, summary_hr_df[['EMPLID', 'earliest_date', 'latest_date']], how='left', on='EMPLID')

# Step 5: Filter the merged dataset to keep only rows where `DATE WORKED` is between `earliest_date` and `latest_date`
filtered_df = merged_df.loc[
    (merged_df['DATE WORKED'] >= merged_df['earliest_date']) &
    (merged_df['DATE WORKED'] <= merged_df['latest_date'])
]

# Step 6: Extract the unique `index` values where the condition is satisfied
unique_indexes = filtered_df['index'].unique()

# Step 7: Add the `is_perm` indicator to the original timesheet data using `.loc`
timesheet_df['is_perm'] = False  # Initialize as False
timesheet_df.loc[timesheet_df['index'].isin(unique_indexes), 'is_perm'] = True  # Update to True for matching rows

# Step 8: Test if the row count has changed after processing
updated_row_count = len(timesheet_df)

# Test to ensure the number of rows hasn't changed
if original_row_count == updated_row_count:
    print(f"Row count check passed: {updated_row_count} rows.")
else:
    print(f"Row count mismatch! Original: {original_row_count}, Updated: {updated_row_count}")

# Step 9: Output the updated timesheet data with the `is_perm` indicator
timesheet_df.to_parquet(output_directory + r'\timesheet_with_student_and_perm_indicator.parquet', index=False)

# Step 10: Output the test sample with the `is_perm` indicator to Excel
timesheet_df.head(2000).to_excel(output_tests + r'\timesheet_with_student_and_perm_indicator_sample.xlsx', index=False)

print("Updated timesheet with `is_perm` indicator saved successfully.")
