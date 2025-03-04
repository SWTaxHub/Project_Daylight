import pandas as pd
import os
import numpy as np
import datetime

# Step 1: Define paths for files and output directories
#timesheet_min_top_up_cals_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet'
#timesheet_min_top_up_cals_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet"
#Penalties_Recalc = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\timesheet_min_top_up_cals_Super.parquet"
Penalties_Recalc = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals_Super.parquet"

#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"
# Step 2: Load the Parquet files
#timesheet_min_top_up_cals = pd.read_parquet(timesheet_min_top_up_cals_path)
Penalties_Recalc = pd.read_parquet(Penalties_Recalc)

# Step 3: Find all unique EMPID_week_id where any of the specified overtime conditions are met in timesheet_cas_OT_daily_weekly
condition_ot = (
    (Penalties_Recalc['ot_callback_hrs'] != 0) |
    (Penalties_Recalc['OT_Cas_Loading_Discrp'] != 0)
)

# Filter for unique EMPID_week_id where overtime conditions are met
empid_week_id_with_overtime = Penalties_Recalc.loc[condition_ot, 'EMPLID_week_id'].unique()

# Step 5: Filter transactions in timesheet_cas_OT_daily_weekly where EMPID_week_id matches those with overtime or where date_only matches those with top-up cash
matching_transactions = Penalties_Recalc[
    (Penalties_Recalc['EMPLID_week_id'].isin(empid_week_id_with_overtime))
]

# step 5b: Restrict extract to only hours worked after 01/01/2017

# Ensure 'DATE WORKED' is in datetime format using .loc
matching_transactions.loc[:, 'DATE WORKED'] = pd.to_datetime(matching_transactions['DATE WORKED'], errors='coerce')

# Filter rows where 'DATE WORKED' is after 01/01/2017
matching_transactions = matching_transactions[matching_transactions['DATE WORKED'] >= '2017-01-01']

# Group by DATE WORKED and EMPLID, and count occurrences
'''
duplicate_combinations = matching_transactions.groupby(['DATE WORKED', 'EMPLID']).size().reset_index(name='Count')

# Filter for combinations that occur more than once
duplicate_combinations = duplicate_combinations[duplicate_combinations['Count'] > 1]

# Display the duplicate combinations
print(duplicate_combinations)

duplicate_combinations.to_csv('potental_rule_breakers.csv')

'''

# Mark combinations of DATE WORKED and EMPLID that occur more than once
matching_transactions['Is_Duplicate'] = matching_transactions.groupby(['DATE WORKED', 'EMPLID'])['EMPLID'].transform('size') > 1

# Filter the DataFrame to keep only rows where the condition is true
condensed_data = matching_transactions[matching_transactions['Is_Duplicate']]



# Display the condensed DataFrame
print(condensed_data)
condensed_data.to_csv('potential_rule_breakers.csv')



# Step 6: Output the matching transactions to Excel as a sample
current_date = datetime.datetime.now().strftime('%Y-%m-%d')
output_file = os.path.join(output_tests, f'test_OTClaimed_vs_Loading_{current_date}.xlsx')
matching_transactions.head(100000).to_excel(output_file, index=False)

print(f"Sample of matching transactions with overtime and top-up cash saved to {output_file}")
# Print the number of rows in the matching_transactions DataFrame
print(f"Number of rows in matching_transactions: {len(matching_transactions)}")

