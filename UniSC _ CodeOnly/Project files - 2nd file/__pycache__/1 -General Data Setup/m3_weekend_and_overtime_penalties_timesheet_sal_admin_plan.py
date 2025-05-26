import pandas as pd
import os  # Import os to handle directory operations
import numpy as np
import time

# Step 1: Define paths for files and output directories
#Commented out Paul's file paths for testing
#timesheet_file = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\cleaned_combined_timesheet_data.parquet'
#hr_summary_file = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\summary_hr_data.parquet'
#ea_base_rates_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\UniSC Data Transfer 2 Sept\EA Base rates.xlsx'
#output_cleaned_data = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\\'
#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'


timesheet_file = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\cleaned_combined_timesheet_data.parquet'
hr_summary_file = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\summary_hr_data.parquet'
ea_base_rates_path = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\UniSC Data Transfer 2 Sept\EA Base rates.xlsx'

output_cleaned_data = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\'
output_tests = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\'




# Ensure the output directories exist
if not os.path.exists(output_cleaned_data):
    os.makedirs(output_cleaned_data)

if not os.path.exists(output_tests):
    os.makedirs(output_tests)

# Step 2: Load the cleaned timesheet and HR summary data from Parquet
timesheet_df = pd.read_parquet(timesheet_file)
hr_summary_df = pd.read_parquet(hr_summary_file)
base_rates_df = pd.read_excel(ea_base_rates_path)



hr_summary_df.to_csv(output_cleaned_data + r'\summary_hr_data.csv', index=False)


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




# Step 3: Add an index column to timesheet data before the join
timesheet_df['index'] = timesheet_df.index  # Add an index column

# Step 4: Create a new column EMPID_EMPL_RCD in timesheet data by concatenating EMPLID and EMPL_RCD as text
timesheet_df['EMPID_EMPL_RCD'] = timesheet_df['EMPLID'].astype(str) + timesheet_df['EMPL_RCD'].astype(str)

# Print the number of rows before the join
print(f"Number of rows in timesheet before the join: {len(timesheet_df)}")

# Step 5: Left join on EMPID_EMPL_RCD and include SAL_ADMIN_PLAN, earliest_date, and latest_date
merged_df = pd.merge(
    timesheet_df,
    hr_summary_df[['EMPID_EMPL_RCD', 'job_code', 'pay_group', 'merged_plan', 'earliest_date', 'latest_date']],  # Columns to keep
    how='left',  # Left join
    on='EMPID_EMPL_RCD'
)



# Print the number of rows after the join
print(f"Number of rows after the join: {len(merged_df)}")

# Rename 'merged_plan' to 'SAL_ADMIN_PLAN'
merged_df.rename(columns={'merged_plan': 'SAL_ADMIN_PLAN'}, inplace=True)

# Step 6: Identify transactions where DATE WORKED is outside the employment period (not between earliest and latest date)
outside_employment_period = merged_df[
    (merged_df['DATE WORKED'] < merged_df['earliest_date']) |
    (merged_df['DATE WORKED'] > merged_df['latest_date']) |
    merged_df['earliest_date'].isnull() | merged_df['latest_date'].isnull()  # Include rows where earliest/latest are null
]

# Step 7: Output transactions "working outside employment period" to an Excel file in the Test folder
if not outside_employment_period.empty:
    test_file = output_tests + 'working_outside_employment_period.xlsx'
    outside_employment_period.to_excel(test_file, index=False)
    print(f"Transactions working outside employment period saved to {test_file}")
else:
    print("No transactions found where DATE WORKED is outside the employment period.")

# Step 9: Check for null values in SAL_ADMIN_PLAN and output to exceptions file if necessary
null_sal_admin_plan = merged_df[merged_df['SAL_ADMIN_PLAN'].isnull()]

if not null_sal_admin_plan.empty:
    # Save null SAL_ADMIN_PLAN records to an exceptions file in the output folder
    exceptions_file = output_tests + 'exceptions_null_SAL_ADMIN_PLAN.xlsx'
    null_sal_admin_plan.to_excel(exceptions_file, index=False)
    print(f"Warning: {len(null_sal_admin_plan)} transactions have SAL_ADMIN_PLAN as null.")
    print(f"Transactions with null SAL_ADMIN_PLAN saved to {exceptions_file}")
else:
    print("No transactions with SAL_ADMIN_PLAN as null.")

# Step 10: Check for duplicate index numbers after the join and output to the Test folder if necessary
duplicate_index = merged_df[merged_df.duplicated(subset=['index'], keep=False)]

if not duplicate_index.empty:
    # Output duplicated index transactions to the Test folder
    duplicate_index_file = output_tests + 'duplicate_index_transactions.xlsx'
    duplicate_index.to_excel(duplicate_index_file, index=False)
    print(f"Warning: {len(duplicate_index)} duplicated transactions found based on index.")
    print(f"Duplicated transactions saved to {duplicate_index_file}")
else:
    print("No duplicated transactions found based on index.")

# Step 11: Extract dates from the column headers and map them to the rates
# Convert the column headings to extract dates for comparison
date_columns = [col for col in base_rates_df.columns if 'Hourly Rate' in col]
date_list = []
invalid_dates = []

for col in date_columns:
    # Extract the date from the column name
    try:
        date_str = col.split(' ')[-1]
        date = pd.to_datetime(date_str, format='%d/%m/%Y')
        date_list.append((col, date))
    except ValueError:
        invalid_dates.append(col)  # Skip columns that don't have a valid date

if invalid_dates:
    print("Invalid date columns:")
    print(invalid_dates)
else:
    print("All date columns are valid.")

# Sort date_list once before applying the function
date_list.sort(key=lambda x: x[1])

# Create lookup dictionary for base rates before applying function
rate_dict = {}
for _, row in base_rates_df.iterrows():
    grade_step = row['Level/Step']
    rates = {col: row[col] for col in date_columns}
    rate_dict[grade_step] = rates


def get_base_rate_optimized(row, rate_dict, date_list):
    grade_step = row['Grade-Step OR Course Code']
    
    # Quick lookup instead of filtering DataFrame
    if grade_step not in rate_dict:
        return np.nan
    
    rates = rate_dict[grade_step]
    date_worked = row['DATE WORKED']
    
    # Binary search through sorted dates
    left, right = 0, len(date_list)
    while left < right:
        mid = (left + right) // 2
        if date_list[mid][1] >= date_worked:
            right = mid
        else:
            left = mid + 1
            
    if left == 0:
        return rates[date_list[0][0]]
    if left == len(date_list):
        return rates[date_list[-1][0]]
    return rates[date_list[left-1][0]]

# Apply optimized function
merged_df['base_rate'] = merged_df.apply(
    lambda row: get_base_rate_optimized(row, rate_dict, date_list), 
    axis=1
)


# Define a function to map the correct rate based on the "DATE WORKED" field in merged_df
def get_base_rate(row, base_rates_df, date_list):
    
    # Find the matching row based on the 'Grade-Step OR Course Code'
    grade_step = row['Grade-Step OR Course Code']

    # Filter base_rates_df to find the correct row for the level/step
    rate_row = base_rates_df[base_rates_df['Level/Step'] == grade_step]

    if rate_row.empty:
        return np.nan  # No match found for the level/step

    # Iterate over the date list and find the appropriate rate based on 'DATE WORKED'
    # Iterate in reverse to pick up the correct rate for earlier periods
    for i, (col, date) in enumerate(date_list):
        if row['DATE WORKED'] <= date:
            # If it's the first column, return it as is
            if i == 0:
                return rate_row[col].values[0]
            # Otherwise, return the rate from the previous column
            return rate_row[date_list[i-1][0]].values[0]

    # If no date condition matched, return the latest rate (i.e., the last available column rate)
    return rate_row[date_list[-1][0]].values[0]

start_time = time.time()

# Process in chunks for better performance
chunk_size = 10000
base_rates = []

for chunk_start in range(0, len(merged_df), chunk_size):
    chunk_end = min(chunk_start + chunk_size, len(merged_df))
    chunk = merged_df.iloc[chunk_start:chunk_end]
    
    chunk_rates = chunk.apply(
        lambda row: get_base_rate_optimized(row, rate_dict, date_list),
        axis=1
    )
    base_rates.extend(chunk_rates)

merged_df['base_rate'] = base_rates

end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")

merged_df_csv = merged_df.copy()

# only show data from June 30th 2024 onwards
merged_df_csv = merged_df_csv[merged_df_csv['DATE WORKED'] >= '2024-01-01']



check_emplids(merged_df, emplids_list, "Merged Data")


# Step 14: Output the final table to Parquet and Excel
merged_df.to_parquet(output_cleaned_data + 'timesheet_include_SAL_ADMIN_PLAN.parquet', index=False)
#merged_df.head(2000).to_excel(output_cleaned_data + 'timesheet_include_SAL_ADMIN_PLAN_sample.xlsx', index=False)
merged_df_csv.to_excel(output_cleaned_data + 'timesheet_include_SAL_ADMIN_PLAN_sample.xlsx', index=False)
print("Final table saved to Parquet and Excel.")

