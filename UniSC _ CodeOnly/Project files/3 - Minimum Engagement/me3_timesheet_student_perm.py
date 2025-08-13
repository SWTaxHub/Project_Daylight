import pandas as pd



# Complete list of EMPLIDs to check against
emplids_list = [

'1065200',
'1082447',
'1084015',
'1086737',
'1095707',
'1110567',
'1111375',
'1111577',
'1115571',
'1117164',
'1117765',
'1121038',
'1124461',
'1126467',
'1132911',
'1134550',
'1138183',
'1140286',
'1150609',
'1150686',
'1155234',
'1157420',
'1159456',
'1161781',
'1164228',
'1166428',
'1167211',
'9001610',
'9006461',
'9006535',
'9009265',
'9009308',
'9009649',
'9010295',
'9011523',
'9011752',
'9011920',
'9012171',
'9012190',
'9012204',
'9012210',
'9012212',
'9012216',
'9012217',
'9012261',
'9012269',
'9012279',
'9012304',
'9012314'
]
# EMPID_EMPL_RCD
def check_emplids(df, emplid_list, label=""):
    present = df[df['EMPLID'].isin(emplid_list)]['EMPLID'].unique()
    missing = set(emplid_list) - set(present)
    
    print(f"\n--- {label} ---")
    print("Found EMPLIDs:", list(present))
    if missing:
        print("Missing EMPLIDs:", list(missing))
    else:
        print("All EMPLIDs found.")



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
summary_hr_df.rename(columns={'EMPID_EMPL_RCD': 'EMPLID'}, inplace=True)

# Shorten the 'column_name' to 7 characters (left to right)

print('summaryHR columns: ')
print(summary_hr_df.columns)

summary_hr_df['EMPLID'] = summary_hr_df['EMPLID'].str[:7]





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


check_emplids(timesheet_df, emplids_list, "Timesheet Data with Perm Indicator")

# Step 9: Output the updated timesheet data with the `is_perm` indicator
timesheet_df.to_parquet(output_directory + r'\timesheet_with_student_and_perm_indicator.parquet', index=False)

# Step 10: Output the test sample with the `is_perm` indicator to Excel
timesheet_df.head(2000).to_excel(output_tests + r'\timesheet_with_student_and_perm_indicator_sample.xlsx', index=False)

print("Updated timesheet with `is_perm` indicator saved successfully.")
