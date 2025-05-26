import pandas as pd
import os  # Import os to handle directory operations
import numpy as np

# Define a test control variable
test_on = 1  # Set to 1 to enable the test, 0 to skip it

# Step 1: Define paths for files and output directories
#timesheet_cas_filtered_rules_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_cas_filtered_rules.parquet'
#output_cleaned_data = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\\'
#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
#rules_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\list_of_dep_and_positions_rules.xlsx'

#Sam's file paths
#Test 1: change file path to not look at condensed data set only including casual professionals
#timesheet_cas_filtered_rules_path = r'c:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_cas_filtered_rules.parquet'
#Keep the name the same for testing purposes change to more accurate one later
timesheet_cas_filtered_rules_path = r'c:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_cas_filtered_rules.parquet'

output_cleaned_data = r'c:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\'
output_tests = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\'
rules_file_path = r'c:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\list_of_dep_and_positions_rules.xlsx'


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







# Step 2: Load the Parquet file
timesheet_cas_OT_weekend_span = pd.read_parquet(timesheet_cas_filtered_rules_path)




print('step 1 done')
# Define the comparison date as a datetime object (no need to convert since DATE WORKED is datetime)
comparison_date = pd.to_datetime('22/11/2023', format='%d/%m/%Y')

# Step 2: Calculate `cal_ot_span_weekend_hours`
timesheet_cas_OT_weekend_span['cal_ot_span_weekend_hours'] = np.where(
    (timesheet_cas_OT_weekend_span['DATE WORKED'] > comparison_date) &  # Condition for DATE WORKED
    (timesheet_cas_OT_weekend_span['DOTW'] < 3) &  # Saturday or Sunday
    (timesheet_cas_OT_weekend_span['Exclude weekends'] == 'y') &  # Exclude weekends == 'y'
    (~timesheet_cas_OT_weekend_span['Grade-Step OR Course Code'].str.contains('L8|L9|L10', na=False)),  # Exclude L8, L9, L10
    timesheet_cas_OT_weekend_span['total_hours'],  # Return total_hours if conditions met
    0  # Otherwise, set to 0
)
print('step 2 done')
# Helper function to calculate the time difference in hours
def calculate_time_difference_in_hours(start_time, rule_time):
    datetime_start = pd.to_datetime(start_time, format='%H:%M:%S')
    datetime_rule = pd.to_datetime(rule_time, format='%H:%M:%S')
    time_difference = (datetime_start - datetime_rule).total_seconds()  # Subtract start from rule for positive time
    return time_difference / 3600  # Convert seconds to hours

# Step 1: Update cal_ot_span_as_hours
timesheet_cas_OT_weekend_span['cal_ot_span_as_hours'] = np.where(
    (timesheet_cas_OT_weekend_span['DATE WORKED'] > comparison_date) &  # Condition for DATE WORKED
    (timesheet_cas_OT_weekend_span['DOTW'] > 2) &  # Weekday: Monday-Friday
    (timesheet_cas_OT_weekend_span['Exclude weekends'] == 'y') &  # Exclude weekends == 'y'
    (timesheet_cas_OT_weekend_span['datetime_endwork'].dt.time > timesheet_cas_OT_weekend_span['Rule - less than time']) &  # Time comparison
    (~timesheet_cas_OT_weekend_span['Grade-Step OR Course Code'].str.contains('L8|L9|L10', na=False)),  # Exclude L8, L9, L10
    # Calculate the difference in hours (positive subtraction)
    timesheet_cas_OT_weekend_span.apply(lambda row: calculate_time_difference_in_hours(row['datetime_endwork'].time(), row['Rule - less than time']), axis=1),
    np.where(
        (timesheet_cas_OT_weekend_span['DATE WORKED'] > comparison_date) &  # Condition for DATE WORKED
        (timesheet_cas_OT_weekend_span['Exclude weekends'] == 'n') &  # Exclude weekends == 'n'
        (timesheet_cas_OT_weekend_span['datetime_endwork'].dt.time > timesheet_cas_OT_weekend_span['Rule - less than time']) &  # Time comparison
        (~timesheet_cas_OT_weekend_span['Grade-Step OR Course Code'].str.contains('L8|L9|L10', na=False)),  # Exclude L8, L9, L10
        # Calculate the difference in hours (positive subtraction)
        timesheet_cas_OT_weekend_span.apply(lambda row: calculate_time_difference_in_hours(row['datetime_endwork'].time(), row['Rule - less than time']), axis=1),
        0  # Set to 0 if no conditions met
    )
)
print('step 3 done')
# Step 2: Cap cal_ot_span_as_hours to total_hours
timesheet_cas_OT_weekend_span['cal_ot_span_as_hours'] = np.where(
    timesheet_cas_OT_weekend_span['cal_ot_span_as_hours'] > timesheet_cas_OT_weekend_span['total_hours'],
    timesheet_cas_OT_weekend_span['total_hours'],  # Cap at total_hours
    timesheet_cas_OT_weekend_span['cal_ot_span_as_hours']  # Otherwise keep the calculated value
)
print('step 4 done')
# Step 3: Update cal_ot_span_bs_hours
timesheet_cas_OT_weekend_span['cal_ot_span_bs_hours'] = np.where(
    (timesheet_cas_OT_weekend_span['DATE WORKED'] > comparison_date) &  # Condition for DATE WORKED
    (timesheet_cas_OT_weekend_span['DOTW'] > 2) &  # Weekday: Monday-Friday
    (timesheet_cas_OT_weekend_span['Exclude weekends'] == 'y') &  # Exclude weekends == 'y'
    (timesheet_cas_OT_weekend_span['datetime_startwork'].dt.time < timesheet_cas_OT_weekend_span['Rule - greater than time']) &  # Time comparison
    (~timesheet_cas_OT_weekend_span['Grade-Step OR Course Code'].str.contains('L8|L9|L10', na=False)),  # Exclude L8, L9, L10
    # Calculate the difference in hours
    timesheet_cas_OT_weekend_span.apply(lambda row: calculate_time_difference_in_hours(row['Rule - greater than time'], row['datetime_startwork'].time()), axis=1),
    np.where(
        (timesheet_cas_OT_weekend_span['DATE WORKED'] > comparison_date) &  # Condition for DATE WORKED
        (timesheet_cas_OT_weekend_span['Exclude weekends'] == 'n') &  # Exclude weekends == 'n'
        (timesheet_cas_OT_weekend_span['datetime_startwork'].dt.time < timesheet_cas_OT_weekend_span['Rule - greater than time']) &  # Time comparison
        (~timesheet_cas_OT_weekend_span['Grade-Step OR Course Code'].str.contains('L8|L9|L10', na=False)),  # Exclude L8, L9, L10
        # Calculate the difference in hours
        timesheet_cas_OT_weekend_span.apply(lambda row: calculate_time_difference_in_hours(row['Rule - greater than time'], row['datetime_startwork'].time()), axis=1),
        0  # Set to 0 if no conditions met
    )
)

print('step 5 done')
# Step 4: Cap cal_ot_span_bs_hours to total_hours
timesheet_cas_OT_weekend_span['cal_ot_span_bs_hours'] = np.where(
    timesheet_cas_OT_weekend_span['cal_ot_span_bs_hours'] > timesheet_cas_OT_weekend_span['total_hours'],
    timesheet_cas_OT_weekend_span['total_hours'],  # Cap at total_hours
    timesheet_cas_OT_weekend_span['cal_ot_span_bs_hours']  # Otherwise keep the calculated value
)

print('step 6 done')

if test_on == 1:
    # Step 1: Filter transactions where 'DATE WORKED' > 22/11/2023
    sample_test = timesheet_cas_OT_weekend_span[
        timesheet_cas_OT_weekend_span['DATE WORKED'] > comparison_date
    ]

    # Step 2: Output the filtered rows to an Excel file
    sample_test_file = os.path.join(output_tests, 'sample_test_transactions_after_22_11_2023_span.xlsx')
    sample_test.to_excel(sample_test_file, index=False)

    print(f"Sample test saved to {sample_test_file}")
else:
    print("Test is turned off. Skipping sample test for transactions after 22/11/2023.")

print('step 7 done')

check_emplids(timesheet_cas_OT_weekend_span, emplids_list, label="Output EMPLIDs in timesheet_cas_OT_weekend_span")

# Step 11: Output the final table to Parquet and Excel
timesheet_cas_OT_weekend_span.to_parquet(output_cleaned_data + 'timesheet_cas_OT_weekend_span.parquet', index=False)
timesheet_cas_OT_weekend_span.head(2000).to_excel(output_cleaned_data + 'timesheet_cas_OT_weekend_span_sample.xlsx', index=False)
print("Final table saved to Parquet and Excel.")