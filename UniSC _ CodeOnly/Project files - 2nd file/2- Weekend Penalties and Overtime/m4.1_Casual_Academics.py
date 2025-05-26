import pandas as pd
import numpy as np
import os

#timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_daily_weekly.parquet"

timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_cas_filtered_rules.parquet"


output_cleaned_data = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"
output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"

# list which is in HR Dataset
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




# Load the timesheet data
timesheet_df = pd.read_parquet(timesheet_path)

'''

# Group by EMPLID and DATE_WORKED, calculate sums, and assign 'Acad/Prof'
def assign_acad_prof(group):
    # Calculate total hours for the group
    total_casualAcad_hours = group['sum_of_casualAcad_hours'].sum()
    total_hours = group['sum_of_hours'].sum()
    
    # Assign 'ACAD' or 'PROF' based on the group-level comparison
    group['Acad/Prof'] = 'ACAD' if total_casualAcad_hours > total_hours else 'PROF'
    return group

# Apply the function to each group
timesheet_df = timesheet_df.groupby(['EMPLID', 'DATE WORKED']).apply(assign_acad_prof)

'''

'''
# Calculate group-level sums
group_sums = timesheet_df.groupby(['EMPLID', 'DATE WORKED'])[['sum_of_casualAcad_hours', 'sum_of_hours']].transform('sum')

# Assign 'Acad/Prof' based on the group-level sums
timesheet_df['Acad/Prof'] = np.where(
    group_sums['sum_of_casualAcad_hours'] > group_sums['sum_of_hours'],
    'ACAD',
    'PROF'
)

'''


# Calculate cumulative sums within each group
timesheet_df['ACAD_cumSum'] = timesheet_df.groupby(['EMPLID', 'DATE WORKED'])['sum_of_casualAcad_hours'].cumsum()
timesheet_df['PROF_cumSum'] = timesheet_df.groupby(['EMPLID', 'DATE WORKED'])['sum_of_hours'].cumsum()

# (Optional) Assign 'Acad/Prof' for comparison based on the cumulative sums
timesheet_df['Acad/Prof'] = np.where(
    timesheet_df['ACAD_cumSum'] > timesheet_df['PROF_cumSum'],
    'ACAD',
    'PROF'
)

# Display the DataFrame with the new columns
print(timesheet_df[['EMPLID', 'DATE WORKED', 'sum_of_casualAcad_hours', 'sum_of_hours', 'ACAD_cumSum', 'PROF_cumSum', 'Acad/Prof']])


# Identify rows where there are multiple unique job_code values for the same EMPLID and DATE_WORKED
job_code_counts = timesheet_df.groupby(['EMPLID', 'DATE WORKED'])['job_code'].nunique()

# Filter combinations with more than one unique job_code
multiple_job_codes = job_code_counts[job_code_counts > 1].index

# Extract rows corresponding to these combinations
timesheet_df_with_multiple_job_codes = timesheet_df[timesheet_df.set_index(['EMPLID', 'DATE WORKED']).index.isin(multiple_job_codes)]

# Display the result
#print(timesheet_df_with_multiple_job_codes)

# Drop rows where job_code == 'SESS'
timesheet_df = timesheet_df[timesheet_df['job_code'] != 'SESS']

# Drop CASACAD Total Hours column
timesheet_df.drop(columns=['CASACAD_total_hours'], inplace=True)

# Display the updated DataFrame
print(timesheet_df)

# Drop rows where job_code == 'SESS'
timesheet_df_with_multiple_job_codes = timesheet_df_with_multiple_job_codes[timesheet_df_with_multiple_job_codes['job_code'] != 'SESS']


check_emplids(timesheet_df, emplids_list, label="Filtered Timesheet Data")

timesheet_df.to_parquet(output_cleaned_data + 'timesheet_cas_filtered_rules.parquet', index=False)


#timesheet_df_with_multiple_job_codes.to_csv('timesheet_df_with_multiple_job_codes.csv')

