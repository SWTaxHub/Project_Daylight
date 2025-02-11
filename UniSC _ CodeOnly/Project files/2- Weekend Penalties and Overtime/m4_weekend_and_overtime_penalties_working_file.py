import pandas as pd
import os  # Import os to handle directory operations
import numpy as np

# Define a test control variable
test_on = 1  # Set to 1 to enable the test, 0 to skip it

# Step 1: Define paths for files and output 
#Comment out Paul's file path for testing purposes: 

# timesheet_sal_admin_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_include_SAL_ADMIN_PLAN.parquet'
# exclusion_list_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\UniSC Data Transfer 2 Sept\Exclusion list.xlsx'
# public_holiday_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\UniSC Data Transfer 2 Sept\Public Holidays.xlsx'
# output_cleaned_data = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\\'
# output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
# rules_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\list_of_dep_and_positions_rules.xlsx'


#Sam's file paths
timesheet_sal_admin_file_path = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_include_SAL_ADMIN_PLAN.parquet'
exclusion_list_path = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\UniSC Data Transfer 2 Sept\Exclusion list.xlsx'
public_holiday_file_path = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\UniSC Data Transfer 2 Sept\Public Holidays.xlsx'
output_cleaned_data = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\'
output_tests = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\'
rules_file_path = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\list_of_dep_and_positions_rules.xlsx'





# Step 2: Load the Parquet file
timesheet_sal_admin_file = pd.read_parquet(timesheet_sal_admin_file_path)

# Replace 'nan' strings in the 'Position Title' column with 'None'
timesheet_sal_admin_file['Position Title'] = timesheet_sal_admin_file['Position Title'].replace("nan", "None")

# Alternatively, if the 'nan' values are NaN (null values), use this:
timesheet_sal_admin_file['Position Title'] = timesheet_sal_admin_file['Position Title'].fillna("None")

# Clean the timesheet_cas_filtered DataFrame
timesheet_sal_admin_file['DEPTID'] = timesheet_sal_admin_file['DEPTID'].str.strip().str.upper()
timesheet_sal_admin_file['Department Name'] = timesheet_sal_admin_file['Department Name'].str.strip().str.upper()
timesheet_sal_admin_file['Position Title'] = timesheet_sal_admin_file['Position Title'].str.strip().str.upper()

# # Step 3: Filter for FULL_PART_TIME == 'D' and save to a new DataFrame
# timesheet_sal_admin_file_casuals_only = timesheet_sal_admin_file[timesheet_sal_admin_file['FULL_PART_TIME'] == 'D']
#
# # Print how many rows after filtering for FULL_PART_TIME == 'D'
# print(f"Number of rows after filtering FULL_PART_TIME == 'D': {len(timesheet_sal_admin_file_casuals_only)}")
#
# # Step 4: Filter for rows where SAL_ADMIN_PLAN contains 'CAS'
# timesheet_cas_filtered = timesheet_sal_admin_file_casuals_only[timesheet_sal_admin_file_casuals_only['SAL_ADMIN_PLAN'].str.contains('CAS', na=False)]
#
# # Print how many rows after filtering for SAL_ADMIN_PLAN containing 'CAS'
# print(f"Number of rows where SAL_ADMIN_PLAN contains 'CAS': {len(timesheet_cas_filtered)}")

# Step 3: Filter for job_code == 'CASUAL', pay_group == 'APT', and SAL_ADMIN_PLAN contains 'CAS' or is 'AGR' *** replaced above code based on updated filtering advice

""" timesheet_cas_filtered = timesheet_sal_admin_file[
    (timesheet_sal_admin_file['job_code'] == 'CASUAL') &
    (timesheet_sal_admin_file['pay_group'] == 'APT') &
    (timesheet_sal_admin_file['SAL_ADMIN_PLAN'].str.contains('CAS', na=False))
]
 """




timesheet_sal_admin_file = timesheet_sal_admin_file[
    (timesheet_sal_admin_file['job_code'] == 'CASUAL') |
    (timesheet_sal_admin_file['job_code'] == 'SESS')
    
    ]
#The above filtering is causing only Casual Professional employees to be pulled in. I need to bring casual academic as well 




# Print the number of rows after filtering
#print(f"Number of rows after applying the new scope rules: {len(timesheet_cas_filtered)}")


# Step 5: Load the Excel file with appropriate data types
rules_df = pd.read_excel(rules_file_path, dtype={
    'DEPTID': str,
    'Department Name': str,
    'Position Title': str,
    'Rule - Weekend Penalty': str,
    'Exclude weekends': str
}, parse_dates=['Rule - greater than date'])


# Convert time fields after loading
rules_df['Rule - greater than time'] = pd.to_datetime(rules_df['Rule - greater than time'], format='%H:%M:%S').dt.time
rules_df['Rule - less than time'] = pd.to_datetime(rules_df['Rule - less than time'], format='%H:%M:%S').dt.time

# Clean the rules_df DataFrame
rules_df['DEPTID'] = rules_df['DEPTID'].str.strip().str.upper()
rules_df['Department Name'] = rules_df['Department Name'].str.strip().str.upper()
rules_df['Position Title'] = rules_df['Position Title'].str.strip().str.upper()

'''
#Commented out on 7/1/24 due to request for total of academic casual hours
# Step 6: Perform a left join on the fields DeptID, Department Name, and Position Title
timesheet_cas_filtered_rules = pd.merge(
    timesheet_cas_filtered,
    rules_df,
    how='left',  # Perform a left join
    on=['DEPTID', 'Department Name', 'Position Title']  # Fields to join on
)

'''

# Step 6: Perform a left join on the fields DeptID, Department Name, and Position Title
timesheet_cas_filtered_rules = pd.merge(
    timesheet_sal_admin_file,
    rules_df,
    how='left',  # Perform a left join
    on=['DEPTID', 'Department Name', 'Position Title']  # Fields to join on
)







# Define default values
default_weekend_penalty = 'n'
default_greater_than_date = pd.to_datetime('27/11/2023', format='%d/%m/%Y')
default_greater_than_time = pd.to_datetime('06:00:00', format='%H:%M:%S').time()
default_less_than_time = pd.to_datetime('18:00:00', format='%H:%M:%S').time()
default_exclude_weekends = 'y'

# Update based on conditions where '> 22/11/2023 Span of Hours' is 'nan' (string) and DEPTID is '5' or '44'
timesheet_cas_filtered_rules.loc[
    (timesheet_cas_filtered_rules['> 22/11/2023 Span of Hours'] == 'nan') &
    (timesheet_cas_filtered_rules['DEPTID'].isin(['5', '44'])),
    ['Rule - Weekend Penalty', 'Rule - greater than time', 'Rule - less than time']
] = ['n', default_greater_than_time, default_less_than_time]

# Update where '> 22/11/2023 Span of Hours' is not 'nan' and DEPTID is '5' or '44'
timesheet_cas_filtered_rules.loc[
    (timesheet_cas_filtered_rules['> 22/11/2023 Span of Hours'] != 'nan') &
    (timesheet_cas_filtered_rules['DEPTID'].isin(['5', '44'])),
    ['Rule - Weekend Penalty', 'Rule - greater than time', 'Rule - less than time']
] = ['y', pd.to_datetime('07:00:00', format='%H:%M:%S').time(), pd.to_datetime('21:00:00', format='%H:%M:%S').time()]

# Step 1: Update null values in the specified columns by re-assigning the updated columns to the DataFrame
timesheet_cas_filtered_rules['Rule - Weekend Penalty'] = timesheet_cas_filtered_rules['Rule - Weekend Penalty'].fillna(default_weekend_penalty)
timesheet_cas_filtered_rules['Rule - greater than date'] = timesheet_cas_filtered_rules['Rule - greater than date'].fillna(default_greater_than_date)
timesheet_cas_filtered_rules['Rule - greater than time'] = timesheet_cas_filtered_rules['Rule - greater than time'].fillna(default_greater_than_time)
timesheet_cas_filtered_rules['Rule - less than time'] = timesheet_cas_filtered_rules['Rule - less than time'].fillna(default_less_than_time)
timesheet_cas_filtered_rules['Exclude weekends'] = timesheet_cas_filtered_rules['Exclude weekends'].fillna(default_exclude_weekends)

# Filter rows where 'Rule - greater than time' is NaN or blank
nan_or_blank_rules = timesheet_cas_filtered_rules[timesheet_cas_filtered_rules['Rule - greater than time'].isna()]

# Print the filtered DataFrame
print(nan_or_blank_rules)

# Print how many rows after filtering for SAL_ADMIN_PLAN containing 'CAS'
print(f"Number of rows after joining rules: {len(timesheet_cas_filtered_rules)}")


# Helper function to convert 12-hour time to 24-hour format
def convert_to_24_hour(hour, minute, am_pm):
    # Handle NaNs by setting them to 0 and flagging the issue
    if pd.isna(hour) or pd.isna(minute) or pd.isna(am_pm):
        return '00:00:00'

    hour = int(hour) if pd.notna(hour) else 0  # Convert to integer or set to 0 if NaN
    minute = int(minute) if pd.notna(minute) else 0  # Convert to integer or set to 0 if NaN

    if am_pm == 'P' and hour != 12:
        hour += 12
    if am_pm == 'A' and hour == 12:
        hour = 0
    return f'{hour:02d}:{minute:02d}:00'


# Step 8: Add flags for missing start and end times more explicitly
def flag_start_null(row):
    return pd.isna(row['G_START_HOUR']) or pd.isna(row['G_START_MINUTE']) or pd.isna(row['G_START_AM_PM'])

def flag_end_null(row):
    return pd.isna(row['G_FINISH_HOUR']) or pd.isna(row['G_FINISH_MINUTE']) or pd.isna(row['G_FINISH_AM_PM'])

# Apply the flagging function for missing start and end times
timesheet_cas_filtered_rules['Start_null'] = timesheet_cas_filtered_rules.apply(flag_start_null, axis=1)
timesheet_cas_filtered_rules['End_null'] = timesheet_cas_filtered_rules.apply(flag_end_null, axis=1)


# Step 6: Create datetime_start and datetime_end

# Create datetime_start
timesheet_cas_filtered_rules['datetime_startwork'] = pd.to_datetime(
    timesheet_cas_filtered_rules['DATE WORKED'].dt.strftime('%Y-%m-%d') + ' ' +
    timesheet_cas_filtered_rules.apply(
        lambda row: convert_to_24_hour(row['G_START_HOUR'], row['G_START_MINUTE'], row['G_START_AM_PM']), axis=1
    )
)

# Create datetime_end and adjust the date if the time is '00:00:00'
timesheet_cas_filtered_rules['datetime_endwork'] = pd.to_datetime(
    timesheet_cas_filtered_rules['DATE WORKED'].dt.strftime('%Y-%m-%d') + ' ' +
    timesheet_cas_filtered_rules.apply(
        lambda row: convert_to_24_hour(row['G_FINISH_HOUR'], row['G_FINISH_MINUTE'], row['G_FINISH_AM_PM']), axis=1
    )
)

# If time is '00:00:00', add one day to datetime_endwork
timesheet_cas_filtered_rules['datetime_endwork'] = timesheet_cas_filtered_rules['datetime_endwork'].apply(
    lambda dt: dt + pd.Timedelta(days=1) if dt.strftime('%H:%M:%S') == '00:00:00' else dt
)


# Step 10: Update BEGINDTTM and ENDDTTM if blank or NaN
timesheet_cas_filtered_rules['BEGINDTTM'] = timesheet_cas_filtered_rules['BEGINDTTM'].combine_first(
    timesheet_cas_filtered_rules['datetime_startwork']
)
timesheet_cas_filtered_rules['ENDDTTM'] = timesheet_cas_filtered_rules['ENDDTTM'].combine_first(
    timesheet_cas_filtered_rules['datetime_endwork']
)

# Step 11: Create the test conditions if test_on is set to 1
if test_on == 1:
    # Define a margin of error (in seconds or minutes, depending on precision)
    margin = pd.Timedelta(seconds=1)  # You can adjust this to allow a certain margin of error

    # Filter rows where the datetime mismatch conditions are met and Start_null/End_null are False
    test_conditions = timesheet_cas_filtered_rules[
        ((abs(
            timesheet_cas_filtered_rules['datetime_startwork'] - timesheet_cas_filtered_rules['BEGINDTTM']) > margin) &
         (~timesheet_cas_filtered_rules['Start_null'])) |
        ((abs(timesheet_cas_filtered_rules['datetime_endwork'] - timesheet_cas_filtered_rules['ENDDTTM']) > margin) &
         (~timesheet_cas_filtered_rules['End_null']))
        ]

    # Step 12: Output test results to Excel
    test_output_file = os.path.join(output_tests, 'test_datetime_mismatch.xlsx')
    test_conditions.to_excel(test_output_file, index=False)

    print(f"Test results for datetime mismatches saved to {test_output_file}")
else:
    print("Test is turned off. Skipping datetime mismatch test.")

# Fill NaN values in G_BREAK_MINUTES with 0
timesheet_cas_filtered_rules['G_BREAK_MINUTES'] = timesheet_cas_filtered_rules['G_BREAK_MINUTES'].fillna(0)

# Step 12: Calculate total hours worked, only if Start_null and End_null are both False
timesheet_cas_filtered_rules['total_hours'] = np.where(
    (~timesheet_cas_filtered_rules['Start_null'] & ~timesheet_cas_filtered_rules['End_null']),  # Condition
    (timesheet_cas_filtered_rules['datetime_endwork'] - timesheet_cas_filtered_rules['datetime_startwork']).dt.total_seconds() / 3600
    - (timesheet_cas_filtered_rules['G_BREAK_MINUTES'] / 60),  # Calculate total hours if condition is True
    timesheet_cas_filtered_rules['UNITS_CLAIMED']  # Otherwise, set total_hours to UNITS_CLAIMED
)


# Step 12b: Calculate total hours worked, only if Start_null and End_null are both False, but call the total hours CASACAD_total hours
timesheet_cas_filtered_rules['CASACAD_total_hours'] = np.where(
    (~timesheet_cas_filtered_rules['Start_null'] & ~timesheet_cas_filtered_rules['End_null']),  # Condition
    (timesheet_cas_filtered_rules['datetime_endwork'] - timesheet_cas_filtered_rules['datetime_startwork']).dt.total_seconds() / 3600
    - (timesheet_cas_filtered_rules['G_BREAK_MINUTES'] / 60),  # Calculate total hours if condition is True
    timesheet_cas_filtered_rules['UNITS_CLAIMED']  # Otherwise, set CASACAD_total_hours to UNITS_CLAIMED
)


# Step: Test block for UNITS_CLAIMED vs Total Hours comparison if test_on is set to 1
if test_on == 1:
    # Define a margin of error (in hours) for the comparison
    margin_of_error = 0.01  # Adjust as needed, e.g., to allow for rounding differences

    # Filter rows where UNITS_CLAIMED is not approximately equal to total_hours
    test_units_claimed = timesheet_cas_filtered_rules[
        abs(timesheet_cas_filtered_rules['UNITS_CLAIMED'] - timesheet_cas_filtered_rules['total_hours']) > margin_of_error
    ]

    # Output the test results to an Excel file
    test_units_claimed_output_file = os.path.join(output_tests, 'test_units_claimed_vs_total_hours.xlsx')
    test_units_claimed.to_excel(test_units_claimed_output_file, index=False)

    print(f"Test results for UNITS_CLAIMED vs Total Hours saved to {test_units_claimed_output_file}")
else:
    print("Tests are turned off. Skipping UNITS_CLAIMED vs Total Hours comparison.")

# Define the list of PIN_NM values you want to create columns for
#pin_nm_list = ['CASUAL', 'OT', 'CALLBACK', 'SHIFT100', 'SHIFT50', 'SHIFT150', 'SATCASUAL', 'SUNCASUAL', 'ADDIT', 'SHIFT15', 'CASUAL-ORD']

# Adding SESS to the list to see academic casuals 

pin_nm_list = ['CASUAL', 'OT', 'CALLBACK', 'SHIFT100', 'SHIFT50', 'SHIFT150', 'SATCASUAL', 'SUNCASUAL', 'ADDIT', 'SHIFT15', 'CASUAL-ORD']

CAS_ACAD_list = ['TUTORSTP2', 'LECTBASIC', 'TUTORRPT', 'TUTORING', 'OTHERACT2', 'TUTORRPT2', 'MARKING',
                      'OTHERACTIV', 'MARKING2', 'LECTRPT', 'LECTDEVEL', 'MARKSUPVR', 'TRCASUAL', 'ADDIT',
                        'LECTSPEC', 'SESSDAY', 'MISCDUTIES', 'MISCDUT2']


# Step 1a: Create columns for each PIN_NM value


for pin_nm in pin_nm_list:
    timesheet_cas_filtered_rules[f'{pin_nm}_hours'] = np.where(
        timesheet_cas_filtered_rules['PIN_NM'] == pin_nm,  # Condition
        timesheet_cas_filtered_rules['total_hours'],  # Set total_hours if PIN_NM matches
        0  # Otherwise, set it to 0
    )



# Step 1b: Create 

timesheet_cas_filtered_rules['CASACAD_total_hours'] = timesheet_cas_filtered_rules['total_hours']

for pin_nm in CAS_ACAD_list:
    timesheet_cas_filtered_rules[f'{pin_nm}_hours'] = np.where(
        timesheet_cas_filtered_rules['PIN_NM'] == pin_nm,  # Condition
        timesheet_cas_filtered_rules['CASACAD_total_hours'],  # Set total_hours if PIN_NM matches
        0  # Otherwise, set it to 0
    )


# Step 2: Add CASUAL-ORD_hours to CASUAL_hours and rename the field as CASUAL_hours
timesheet_cas_filtered_rules['CASUAL_hours'] = (
    timesheet_cas_filtered_rules['CASUAL_hours'] + timesheet_cas_filtered_rules['CASUAL-ORD_hours']
)


# Step 3: Update CALLBACK_hours to ensure a minimum of 2 hours if CALLBACK_hours is greater than 0 and less than 2
timesheet_cas_filtered_rules['CALLBACK_hours'] = np.where(
    (timesheet_cas_filtered_rules['CALLBACK_hours'] > 0) & (timesheet_cas_filtered_rules['CALLBACK_hours'] < 2),  # Check if CALLBACK_hours is > 0 and < 2
    2,  # Set to 2 hours if the condition is met
    timesheet_cas_filtered_rules['CALLBACK_hours']  # Otherwise, keep the original value
)

# Optionally, drop the CASUAL-ORD_hours column if it's no longer needed
timesheet_cas_filtered_rules.drop(columns=['CASUAL-ORD_hours'], inplace=True)



if test_on == 1:
    # Step 1: List of newly created hour columns from PIN_NM values
    #hour_columns = ['CASUAL_hours', 'OT_hours', 'CALLBACK_hours', 'SHIFT100_hours', 'SHIFT50_hours',
     #               'SHIFT150_hours', 'SATCASUAL_hours', 'SUNCASUAL_hours', 'ADDIT_hours', 'SHIFT15_hours']
    
    # List just contains Cas professionals
    hour_columns = ['CASUAL_hours', 'OT_hours', 'CALLBACK_hours', 'SHIFT100_hours', 'SHIFT50_hours',
                    'SHIFT150_hours', 'SATCASUAL_hours', 'SUNCASUAL_hours', 'ADDIT_hours', 'SHIFT15_hours']
    

 # Adding SESS_hours to the list to see academic casuals 
    CAS_ACAD_list = ['TUTORSTP2_hours', 'LECTBASIC_hours', 'TUTORRPT_hours', 'TUTORING_hours', 'OTHERACT2_hours', 'TUTORRPT2_hours', 'MARKING_hours',
                      'OTHERACTIV_hours', 'MARKING2_hours', 'LECTRPT_hours', 'LECTDEVEL_hours', 'MARKSUPVR_hours', 'TRCASUAL_hours', 'ADDIT_hours',
                        'LECTSPEC_hours', 'SESSDAY_hours', 'MISCDUTIES_hours', 'MISCDUT2_hours']


    # Step 2: Ensure that all necessary columns are present, or initialize them with 0 if not
    #Commented out on 8/01/24 for testing purposes - trying to get SESS hours to update
    # for col in hour_columns:
    #     if col not in timesheet_cas_filtered_rules.columns:
    #         timesheet_cas_filtered_rules[col] = 0  # If the column doesn't exist, initialize with 0


    #Test for creating condition on CAS ACAD Vs CAS Prof hours
    timesheet_cas_filtered_rules['sum_of_casualAcad_hours'] = timesheet_cas_filtered_rules[CAS_ACAD_list].sum(axis=1)


    # Step 3: Calculate the sum of the new hour columns
    timesheet_cas_filtered_rules['sum_of_hours'] = timesheet_cas_filtered_rules[hour_columns].sum(axis=1)

    # Step 4: Test to ensure that sum_of_hours equals total_hours
    test_sum_hours = timesheet_cas_filtered_rules[
        np.round(timesheet_cas_filtered_rules['sum_of_hours'], 4) != np.round(timesheet_cas_filtered_rules['total_hours'], 4)
    ]

    # Step 5: Output the test results to an Excel file if there are discrepancies
    if not test_sum_hours.empty:
        test_output_file = os.path.join(output_tests, 'test_sum_of_hours_vs_total_hours.xlsx')
        test_sum_hours.to_excel(test_output_file, index=False)
        print(f"Test results saved to {test_output_file}")
    else:
        print("Test passed: All sums match total_hours.")
else:
    print("Test is turned off. Skipping sum of hours vs total_hours test.")

# Define the comparison date as a datetime object (no need to convert since DATE WORKED is datetime)
comparison_date = pd.to_datetime('22/11/2023', format='%d/%m/%Y')

if test_on == 1:
    # Step: Test to filter rows where DATE WORKED > 22/11/2023, Rule - Weekend Penalty == 'y', and Start_null or End_null is True
    test_conditions = timesheet_cas_filtered_rules[
        (timesheet_cas_filtered_rules['DATE WORKED'] > comparison_date) &  # Condition for DATE WORKED
        (timesheet_cas_filtered_rules['Rule - Weekend Penalty'] == 'y') &  # Condition for Rule - Weekend Penalty
        (timesheet_cas_filtered_rules['Start_null'] | timesheet_cas_filtered_rules['End_null'])  # Start_null or End_null is True
    ]

    # Step: Output the test results to an Excel file if there are any rows matching the conditions
    if not test_conditions.empty:
        test_output_file = os.path.join(output_tests, 'test_weekend_penalty_start_end_null.xlsx')
        test_conditions.to_excel(test_output_file, index=False)
        print(f"Test results saved to {test_output_file}")
    else:
        print("Test passed: No rows found where conditions were met.")
else:
    print("Test is turned off. Skipping weekend penalty and null start/end times test.")

# Step 1: Add `DOTW` (Day of the Week) where Saturday is 1 and Friday is 7
# Step 1: Map days of the week using the desired mapping
day_mapping = {0: 3, 1: 4, 2: 5, 3: 6, 4: 7, 5: 1, 6: 2}

# Create the DOTW column and apply the mapping
timesheet_cas_filtered_rules['DOTW'] = timesheet_cas_filtered_rules['DATE WORKED'].dt.weekday.map(day_mapping)

# Load the exclusion list
exclusion_df = pd.read_excel(exclusion_list_path, dtype={
    'EMPID_EMPL_RCD': str
})

# Ensure exclusion dates are in datetime format
exclusion_df['start_excl_date'] = pd.to_datetime(exclusion_df['start_excl_date'])
exclusion_df['end_excl_date'] = pd.to_datetime(exclusion_df['end_excl_date'])

# Step 1: Check row count before join
rows_before_join = len(timesheet_cas_filtered_rules)
print(f"Row count before join: {rows_before_join}")

# Step 1: Merge or check exclusions on EMPID_EMPL_RCD and DATE WORKED
# We will use a left join with the exclusion list to add manual_excl flags
timesheet_cas_filtered_rules = pd.merge(
    timesheet_cas_filtered_rules,
    exclusion_df[['EMPID_EMPL_RCD', 'start_excl_date', 'end_excl_date']],
    how='left',
    on='EMPID_EMPL_RCD'
)

# Step 3: Check row count after join
rows_after_join = len(timesheet_cas_filtered_rules)
print(f"Row count after join: {rows_after_join}")

# Step 2: Add a new column 'manual_excl' to flag rows where exclusion applies
timesheet_cas_filtered_rules['manual_excl'] = timesheet_cas_filtered_rules.apply(
    lambda row: True if pd.notnull(row['start_excl_date']) and
                      (row['start_excl_date'] <= row['DATE WORKED'] <= row['end_excl_date'])
               else False,
    axis=1
)

# Step 2: Load the public holiday data from the Excel sheet
public_holidays_df = pd.read_excel(public_holiday_file_path, sheet_name='All PH', usecols=['Date', 'Holiday'])

# Step 3: Check for invalid date values before converting
invalid_dates = public_holidays_df[~pd.to_datetime(public_holidays_df['Date'], errors='coerce').notna()]

if not invalid_dates.empty:
    print("Invalid date values found in the Public Holidays dataset:")
    print(invalid_dates)

# Step 4: Convert valid dates in 'Date' to datetime format, setting errors='coerce' to handle any further invalid dates
public_holidays_df['Date'] = pd.to_datetime(public_holidays_df['Date'], errors='coerce')

# Step 5: Drop rows where the 'Date' could not be parsed
public_holidays_df = public_holidays_df.dropna(subset=['Date'])


# Step 1: Check row count before join
rows_before_join = len(timesheet_cas_filtered_rules)
print(f"Row count before join: {rows_before_join}")

# Step 4: Merge the public holidays onto the timesheet data based on 'DATE WORKED'
# Left join to retain all rows from timesheet_df and add the 'Holiday' field
timesheet_cas_filtered_rules = pd.merge(
    timesheet_cas_filtered_rules,
    public_holidays_df[['Date','Holiday']],
    left_on='DATE WORKED',
    right_on='Date',
    how='left'
)

# Step 5: Fill NA values in 'Holiday' with 'Not a Holiday'
timesheet_cas_filtered_rules['Holiday'] = timesheet_cas_filtered_rules['Holiday'].fillna('Not a Holiday')

# Step 3: Check row count after join
rows_after_join = len(timesheet_cas_filtered_rules)
print(f"Row count after join: {rows_after_join}")

# Step 11: Output the final table to Parquet and Excel
timesheet_cas_filtered_rules.to_parquet(output_cleaned_data + 'timesheet_cas_filtered_rules.parquet', index=False)
timesheet_cas_filtered_rules.head(2000).to_excel(output_cleaned_data + 'timesheet_cas_filtered_rules_sample.xlsx', index=False)
print("Final table saved to Parquet and Excel.")

# Step 6: Filter for rows where 'manual_excl' is TRUE
manual_excl_true_df = timesheet_cas_filtered_rules[timesheet_cas_filtered_rules['manual_excl'] == True]

# Step 7: Save the filtered sample to an Excel file
manual_excl_true_df.head(2000).to_excel(output_cleaned_data + 'manual_excl_sample.xlsx', index=False)
print(f"Sample where 'manual_excl' is TRUE saved to: manual_excl_sample.xlsx")

# Step 6: Filter for rows where it is a holiday
holiday_sample_df = timesheet_cas_filtered_rules[timesheet_cas_filtered_rules['Holiday'] != 'Not a Holiday']

# Step 7: Save the filtered sample to an Excel file
holiday_sample_df.head(2000).to_excel(output_cleaned_data + 'Holiday_sample.xlsx', index=False)
print(f"Sample where there public holidays is saved to: Holiday_sample.xlsx")







