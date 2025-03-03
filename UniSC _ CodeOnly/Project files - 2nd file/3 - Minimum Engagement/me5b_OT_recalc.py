import pandas as pd
import numpy as np
import os


# Define file paths
#Paul's file paths
#timesheet_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_with_student_and_perm_indicator.parquet'
#output_cleaned_data = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\\'
#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'


#Sam's file paths

#timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_with_student_and_perm_indicator.parquet"

timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals_Super.parquet"

output_cleaned_data = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"

output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"


# Load the timesheet data
timesheet_df = pd.read_parquet(timesheet_path)

timesheet_df['DATE WORKED'] = pd.to_datetime(timesheet_df['DATE WORKED'], errors='coerce')

loading_factor = 1 - (1 / 1.25)

# Define the cutoff date
cutoff_date = pd.Timestamp('2024-06-30')  # No need for explicit format

# Apply the conditions
timesheet_df.loc[timesheet_df['DATE WORKED'] <= cutoff_date, 'OT_Cas_Loading_Discrp'] = (
    (timesheet_df['cal_PH_ot'] * 2.50) +
    (timesheet_df['cal_sunday_ot'] * 2.00) +
    (timesheet_df['cal_first_3_ot'] * 1.50) +
    (timesheet_df['cal_post_3_ot'] * 2.00)
) * timesheet_df['base_rate'] * loading_factor

timesheet_df.loc[timesheet_df['DATE WORKED'] > cutoff_date, 'OT_Cas_Loading_Discrp'] = (
    (timesheet_df['ts_ot_ph'] * 2.50) +
    (timesheet_df['ts_ot_sunday'] * 2.00) +
    (timesheet_df['ts_ot_first_three'] * 1.50) +
    (timesheet_df['ts_ot_post_three'] * 2.00)
) * timesheet_df['base_rate'] * loading_factor


# Interest on OT CAS LOADING DISCRP

timesheet_df['OT_Cas_Loading_Discrp_withInterest'] = np.where(
    timesheet_df['OT_Cas_Loading_Discrp'].notna() & (timesheet_df['OT_Cas_Loading_Discrp'] != 0), 
    timesheet_df['OT_Cas_Loading_Discrp'] * (1 + timesheet_df['compInterestFactor']), 
    0
)


# 'ts_ot_ph' - all 0 
# 'ts_ot_sunday' - all 0
# 'ts_ot_first_three' - all 0
# 'ts_ot_post_three' - all 0 






# timesheet_df['OT_Cas_Loading_Discrp'] = ((timesheet_df['ts_ot_ph'] * 2.50) + 
#                           (timesheet_df['ts_ot_sunday'] * 2.00) +
#                           (timesheet_df['ts_ot_first_three'] * 1.50) +
#                           (timesheet_df['ts_ot_post_three'] * 2.00)
# ) * timesheet_df['base_rate'] * (1 - (1/1.25))


timesheet_df.to_parquet(output_cleaned_data + 'timesheet_min_top_up_cals_Super.parquet', index=False)