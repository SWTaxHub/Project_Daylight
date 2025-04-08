import pandas as pd
import os  # Import os to handle directory operations
import numpy as np

# Define a test control variable
test_on = 1  # Set to 1 to enable the test, 0 to skip it

# Step 1: Define paths for files and output directories
#commented out Paul's file path
#timesheet_cas_OT_weekend_span_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_weekend_span.parquet'
timesheet_cas_OT_weekend_span_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_weekend_span.parquet"

#Commented out Paul's file path - Query DQ11
#output_cleaned_data = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\\'
output_cleaned_data = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\'


#Commented out Paul's file path - Query DQ11
#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
output_tests = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\'

# Step 2: Load the Parquet file
timesheet_cas_OT_daily_weekly = pd.read_parquet(timesheet_cas_OT_weekend_span_path)

# Step 3: Sort by EMPLID and datetime_startwork (ascending)
#timesheet_cas_OT_daily_weekly.sort_values(by=['EMPLID', 'datetime_startwork'], inplace=True)

# Step to identify rows with duplicate worked times based on given conditions
# Step 3: Sort by EMPLID and datetime_startwork (ascending)
timesheet_cas_OT_daily_weekly.sort_values(by=['EMPLID', 'datetime_startwork', 'job_code'], inplace=True)

# Step A: Add an identifier combining EMPID, EMPL_RCD, BEGINDTTM, and ENDDTTM for easy grouping
timesheet_cas_OT_daily_weekly['EMPID_EMPL_RCD'] = (
    timesheet_cas_OT_daily_weekly['EMPLID'].astype(str) + '_' +
    timesheet_cas_OT_daily_weekly['EMPL_RCD'].astype(str)
)

# Step B: Identify duplicate worked time rows based on 'EMPID_EMPL_RCD', 'BEGINDTTM', and 'ENDDTTM'
duplicate_condition = (
    timesheet_cas_OT_daily_weekly.duplicated(subset=['EMPID_EMPL_RCD', 'BEGINDTTM', 'ENDDTTM'], keep=False) &
    timesheet_cas_OT_daily_weekly['BEGINDTTM'].notna() & timesheet_cas_OT_daily_weekly['ENDDTTM'].notna()
)

# Create a new column 'penalty_reptm' to store the tag based on criteria
timesheet_cas_OT_daily_weekly['penalty_reptm'] = np.where(duplicate_condition, 'duplicate', '')

# Step C: Tag lines within identified duplicates based on PIN_NM
# 1. Tag 'to be deleted' if PIN_NM is 'CASUAL' in duplicate lines
timesheet_cas_OT_daily_weekly.loc[
    (timesheet_cas_OT_daily_weekly['penalty_reptm'] == 'duplicate') &
    (timesheet_cas_OT_daily_weekly['PIN_NM'] == 'CASUAL'), 'penalty_reptm'
] = 'to be deleted'

# 2. Tag 'paid correctly' if PIN_NM contains 'shift' in duplicate lines
timesheet_cas_OT_daily_weekly.loc[
    (timesheet_cas_OT_daily_weekly['penalty_reptm'] == 'duplicate') &
    (timesheet_cas_OT_daily_weekly['PIN_NM'].str.contains('shift', case=False, na=False)), 'penalty_reptm'
] = 'paid correctly'

# Step D: Outside duplicates, tag lines where PIN_NM contains 'shift' but does not meet other criteria
timesheet_cas_OT_daily_weekly.loc[
    (timesheet_cas_OT_daily_weekly['penalty_reptm'] == '') &
    (timesheet_cas_OT_daily_weekly['PIN_NM'].str.contains('shift', case=False, na=False)), 'penalty_reptm'
] = 'no base paid'

# Step E: Export all rows with any tagging to an Excel file for review
tagged_rows = timesheet_cas_OT_daily_weekly[timesheet_cas_OT_daily_weekly['penalty_reptm'] != '']
test_file_path = os.path.join(output_tests, 'tagged_duplicate_timesheet_rows.xlsx')
tagged_rows.to_excel(test_file_path, index=False)
print(f"Tagged duplicate rows saved to: {test_file_path}")

# Step F: Remove rows tagged as 'to be deleted' from the main data table
timesheet_cas_OT_daily_weekly = timesheet_cas_OT_daily_weekly[timesheet_cas_OT_daily_weekly['penalty_reptm'] != 'to be deleted']

# Reset 'penalty_reptm' to indicate rows that are confirmed as unique
# (filtering is required to match the length of the DataFrame after deletion)
timesheet_cas_OT_daily_weekly['penalty_reptm'] = np.where(
    (timesheet_cas_OT_daily_weekly['penalty_reptm'] == '') & ~timesheet_cas_OT_daily_weekly.index.isin(tagged_rows.index),
    'unique',
    timesheet_cas_OT_daily_weekly['penalty_reptm']
)


# # DAILY CALCULATION
# # Step 4: Create a conditional cumulative sum of total hours for daily thresholds
# # Use groupby on EMPLID and the date part of datetime_startwork to get daily cumulative total hours
timesheet_cas_OT_daily_weekly['date_only'] = timesheet_cas_OT_daily_weekly['datetime_startwork'].dt.date
# timesheet_cas_OT_daily_weekly['daily_hrs_count'] = timesheet_cas_OT_daily_weekly.groupby(
#     ['EMPLID', 'date_only'])['total_hours'].cumsum()




#Revised code for Step 4 Daily code  - Query DQ11

# Apply the conditional logic: If PIN_NM equals 0 then adjusted_daily_hours = total_hours otherwise calculate as before
timesheet_cas_OT_daily_weekly['adjusted_daily_hours'] = (
    timesheet_cas_OT_daily_weekly['total_hours'] - 
    timesheet_cas_OT_daily_weekly['cal_ot_span_weekend_hours'] - 
    timesheet_cas_OT_daily_weekly['cal_ot_span_as_hours'] - 
    timesheet_cas_OT_daily_weekly['cal_ot_span_bs_hours']
)




# Step 5: Apply cumsum() on the adjusted hours for daily thresholds
timesheet_cas_OT_daily_weekly['daily_hrs_count'] = timesheet_cas_OT_daily_weekly.groupby(
    ['EMPLID', 'date_only']
)['adjusted_daily_hours'].cumsum()

# Step 5: Create the new field cal_daily_ot_hours (where daily_hrs_count > 10)
timesheet_cas_OT_daily_weekly['cal_daily_ot_hours'] = np.where(
    timesheet_cas_OT_daily_weekly['daily_hrs_count'] > 10,
    timesheet_cas_OT_daily_weekly['daily_hrs_count'] - 10,
    0
)

# Step 6: Calculate incremental daily OT hours
# Group by EMPLID and date, and subtract the prior row's cal_daily_ot_hours to get the incremental increase
timesheet_cas_OT_daily_weekly['incremental_daily_ot_hours'] = timesheet_cas_OT_daily_weekly.groupby(
    ['EMPLID', 'date_only']
)['cal_daily_ot_hours'].diff().fillna(timesheet_cas_OT_daily_weekly['cal_daily_ot_hours'])

# WEEKLY CALCULATION

# Step 1: Ensure 'datetime_startwork' is in datetime format if not already
timesheet_cas_OT_daily_weekly['datetime_startwork'] = pd.to_datetime(timesheet_cas_OT_daily_weekly['datetime_startwork'])

# Step 2: Determine the week start date (Saturday) for each transaction
# If DOTW == 1 (Saturday), it's the start of the week. Subtract DOTW - 1 to get the start of the week.
timesheet_cas_OT_daily_weekly['week_start_date'] = timesheet_cas_OT_daily_weekly['datetime_startwork'] - pd.to_timedelta(
    timesheet_cas_OT_daily_weekly['DOTW'] - 1, unit='D'
)

# Step 3: Create a unique week ID by concatenating 'EMPLID' and the 'week_start_date'
# This will act as the unique identifier for each employee's work week
timesheet_cas_OT_daily_weekly['EMPLID_week_id'] = (
    timesheet_cas_OT_daily_weekly['EMPLID'].astype(str) + '_' + timesheet_cas_OT_daily_weekly['week_start_date'].dt.strftime('%Y-%m-%d')
)

# # Step 4: Calculate the weekly cumulative sum of total hours using 'EMPLID_week_id'
# # We group by 'EMPLID_week_id' to ensure the cumulative sum is calculated within the same week for each employee
# timesheet_cas_OT_daily_weekly['weekly_hrs_count'] = timesheet_cas_OT_daily_weekly.groupby('EMPLID_week_id')['total_hours'].cumsum()



# Revised code for Step 4 Weekly Calc - Query DQ11
# Apply the conditional logic: If PIN_NM equals 0 then adjusted_daily_hours = total_hours otherwise calculate as before
timesheet_cas_OT_daily_weekly['adjusted_weekly_hours'] = (
    timesheet_cas_OT_daily_weekly['total_hours']
    - timesheet_cas_OT_daily_weekly['cal_ot_span_weekend_hours']
    - timesheet_cas_OT_daily_weekly['cal_ot_span_as_hours']
    - timesheet_cas_OT_daily_weekly['cal_ot_span_bs_hours']
    - timesheet_cas_OT_daily_weekly['incremental_daily_ot_hours']
)





# Step 5: Apply cumsum() on the adjusted hours for weekly thresholds
timesheet_cas_OT_daily_weekly['weekly_hrs_count'] = timesheet_cas_OT_daily_weekly.groupby('EMPLID_week_id')[
    'adjusted_weekly_hours'
].cumsum()


# Step 5: Create the new field cal_weekly_ot_hours where the weekly cumulative hours exceed the threshold (36.25 hours)
timesheet_cas_OT_daily_weekly['cal_weekly_ot_hours'] = np.where(
    timesheet_cas_OT_daily_weekly['weekly_hrs_count'] > 36.25,
    timesheet_cas_OT_daily_weekly['weekly_hrs_count'] - 36.25,
    0
)

# Step 6: Calculate incremental weekly OT hours
# Group by EMPLID_week_id, and subtract the prior row's cal_weekly_ot_hours to get the incremental increase
timesheet_cas_OT_daily_weekly['incremental_weekly_ot_hours'] = timesheet_cas_OT_daily_weekly.groupby(
    ['EMPLID_week_id']
)['cal_weekly_ot_hours'].diff().fillna(timesheet_cas_OT_daily_weekly['cal_weekly_ot_hours'])


# Step 7: Calculate cal_OT_hours as the sum of all overtime fields
timesheet_cas_OT_daily_weekly['cal_OT_hours'] = (
    timesheet_cas_OT_daily_weekly['incremental_daily_ot_hours'] +
    timesheet_cas_OT_daily_weekly['incremental_weekly_ot_hours'] +
    timesheet_cas_OT_daily_weekly['cal_ot_span_weekend_hours'] +
    timesheet_cas_OT_daily_weekly['cal_ot_span_as_hours'] +
    timesheet_cas_OT_daily_weekly['cal_ot_span_bs_hours']
)


# Define the fields to set to zero if "Grade-Step OR Course Code" contains "L8", "L9", or "L10"
fields_to_zero = [
    'cal_OT_hours', 'incremental_daily_ot_hours', 'incremental_weekly_ot_hours',
    'cal_ot_span_weekend_hours', 'cal_ot_span_as_hours', 'cal_ot_span_bs_hours'
]

# Apply condition to set specified fields to zero if "Grade-Step OR Course Code" contains "L8", "L9", or "L10"
timesheet_cas_OT_daily_weekly.loc[
    timesheet_cas_OT_daily_weekly['Grade-Step OR Course Code'].str.contains('L8|L9|L10', na=False),
    fields_to_zero
] = 0

print("Fields updated to 0 for rows with 'Grade-Step OR Course Code' containing L8, L9, or L10.")

# New Field: cal_sunday_ot
# Step 1: Set `cal_sunday_ot` where DOTW == 2 (Sunday)
timesheet_cas_OT_daily_weekly['cal_sunday_ot'] = np.where(
    timesheet_cas_OT_daily_weekly['DOTW'] == 2,
    timesheet_cas_OT_daily_weekly['cal_OT_hours'],
    0
)

# New Field: cal_PH_ot
# Step 1: Set `cal_PH_ot` where Holiday != Not a Holiday
timesheet_cas_OT_daily_weekly['cal_PH_ot'] = np.where(
    timesheet_cas_OT_daily_weekly['Holiday'] != 'Not a Holiday',
    timesheet_cas_OT_daily_weekly['cal_OT_hours'],
    0
)


# New Field: cummulative_cal_OT_hours
# Step 2: Calculate cumulative total of cal_OT_hours for each day (excluding Sunday - DOTW != 2 and Holiday != Not a Holiday)
timesheet_cas_OT_daily_weekly['cummulative_cal_OT_hours'] = timesheet_cas_OT_daily_weekly.groupby(
    ['EMPLID', 'date_only']
)['cal_OT_hours'].cumsum()

# Set cumulative OT to 0 where DOTW == 2 (Sunday)
timesheet_cas_OT_daily_weekly['cummulative_cal_OT_hours'] = np.where(
    timesheet_cas_OT_daily_weekly['DOTW'] == 2,
    0,
    timesheet_cas_OT_daily_weekly['cummulative_cal_OT_hours']
)

# Set cumulative OT to 0 where Holiday != Not a Holiday
timesheet_cas_OT_daily_weekly['cummulative_cal_OT_hours'] = np.where(
    timesheet_cas_OT_daily_weekly['Holiday'] != 'Not a Holiday',
    0,
    timesheet_cas_OT_daily_weekly['cummulative_cal_OT_hours']
)

# New Field: prior_cummulative_cal_OT_hours
# Step 3: Calculate prior cumulative OT for the same EMPLID and date_only
timesheet_cas_OT_daily_weekly['prior_cummulative_cal_OT_hours'] = timesheet_cas_OT_daily_weekly.groupby(
    ['EMPLID', 'date_only']
)['cummulative_cal_OT_hours'].shift(1).fillna(0)

# New Field: cal_first_3_ot
# Step 4: Logic for the first 3 OT hours
timesheet_cas_OT_daily_weekly['cal_first_3_ot'] = np.select(
    [
        timesheet_cas_OT_daily_weekly['cummulative_cal_OT_hours'] <= 3,
        (timesheet_cas_OT_daily_weekly['cummulative_cal_OT_hours'] > 3) & (timesheet_cas_OT_daily_weekly['prior_cummulative_cal_OT_hours'] > 3),
        (timesheet_cas_OT_daily_weekly['cummulative_cal_OT_hours'] > 3) & (timesheet_cas_OT_daily_weekly['prior_cummulative_cal_OT_hours'] < 3)
    ],
    [
        timesheet_cas_OT_daily_weekly['cummulative_cal_OT_hours'] - timesheet_cas_OT_daily_weekly['prior_cummulative_cal_OT_hours'],
        0,
        3 - timesheet_cas_OT_daily_weekly['prior_cummulative_cal_OT_hours']
    ],
    default=0
)

# New Field: cal_post_3_ot
# Step 5: Calculate post-3 OT hours for non-Sundays
timesheet_cas_OT_daily_weekly['cal_post_3_ot'] = np.where(
    (timesheet_cas_OT_daily_weekly['DOTW'] != 2) & (timesheet_cas_OT_daily_weekly['Holiday'] == 'Not a Holiday'),
    timesheet_cas_OT_daily_weekly['cal_OT_hours'] - timesheet_cas_OT_daily_weekly['cal_first_3_ot'],
    0
)

# New Field: cal_wknd_penalty_sat
# Step 6: Calculate weekend penalty for Saturday (DOTW == 1, Rule - Weekend Penalty == 'y', and DATE WORKED >= Rule - greater than date)
timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sat'] = np.where(
    (timesheet_cas_OT_daily_weekly['DOTW'] == 1) &
    # 1/04/2025 Added check to make sure DEPTID is not equal to '84'
    (timesheet_cas_OT_daily_weekly['DEPTID'] != '84') &
    (timesheet_cas_OT_daily_weekly['Rule - Weekend Penalty'] == 'y') &
    (timesheet_cas_OT_daily_weekly['DATE WORKED'] >= timesheet_cas_OT_daily_weekly['Rule - greater than date']),
    timesheet_cas_OT_daily_weekly['total_hours'] - timesheet_cas_OT_daily_weekly['cal_OT_hours'],
    0
)

# New Field: cal_wknd_penalty_sun
# Step 7: Calculate weekend penalty for Sunday (DOTW == 2, Rule - Weekend Penalty == 'y', and DATE WORKED >= Rule - greater than date)
timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sun'] = np.where(
    (timesheet_cas_OT_daily_weekly['DOTW'] == 2) &
    # 1/04/2025 Added check to make sure DEPTID is not equal to '84'
    (timesheet_cas_OT_daily_weekly['DEPTID'] != '84') &
    (timesheet_cas_OT_daily_weekly['Rule - Weekend Penalty'] == 'y') &
    (timesheet_cas_OT_daily_weekly['DATE WORKED'] >= timesheet_cas_OT_daily_weekly['Rule - greater than date']),
    timesheet_cas_OT_daily_weekly['total_hours'] - timesheet_cas_OT_daily_weekly['cal_OT_hours'],
    0
)



# Step 7: Create OT split fields based on conditions

timesheet_cas_OT_daily_weekly['ot_callback_hrs'] = timesheet_cas_OT_daily_weekly['CALLBACK_hours'] + timesheet_cas_OT_daily_weekly['OT_hours']

# 1. If Holiday != 'Not a Holiday', then ts_ot_ph
timesheet_cas_OT_daily_weekly['ts_ot_ph'] = np.where(
    timesheet_cas_OT_daily_weekly['Holiday'] != 'Not a Holiday',
    timesheet_cas_OT_daily_weekly['ot_callback_hrs'],
    0
)

# 2. If DOTW == 2 (Sunday), then ts_ot_sunday = OT_hours
timesheet_cas_OT_daily_weekly['ts_ot_sunday'] = np.where(
    timesheet_cas_OT_daily_weekly['DOTW'] == 2,
    timesheet_cas_OT_daily_weekly['ot_callback_hrs'],
    0
)

# 3. Calculate ts_ot_first_three and ts_ot_post_three for non-Sundays and non-public holidays
# Step 3a: Calculate cumulative OT for non-Sundays and non-public holidays
timesheet_cas_OT_daily_weekly['cummulative_OT_hours'] = timesheet_cas_OT_daily_weekly.groupby(
    ['EMPLID', 'date_only']
)['ot_callback_hrs'].cumsum()

# Reset cumulative OT to 0 where it's Sunday or a public holiday
timesheet_cas_OT_daily_weekly['cummulative_OT_hours'] = np.where(
    (timesheet_cas_OT_daily_weekly['DOTW'] == 2) |
    (timesheet_cas_OT_daily_weekly['Holiday'] != 'Not a Holiday'),
    0,
    timesheet_cas_OT_daily_weekly['cummulative_OT_hours']
)

# Step 3b: Calculate prior cumulative OT
timesheet_cas_OT_daily_weekly['prior_cummulative_OT_hours'] = timesheet_cas_OT_daily_weekly.groupby(
    ['EMPLID', 'date_only']
)['cummulative_OT_hours'].shift(1).fillna(0)

# Step 4: Logic for ts_ot_first_three (first 3 hours)
timesheet_cas_OT_daily_weekly['ts_ot_first_three'] = np.select(
    [
        timesheet_cas_OT_daily_weekly['cummulative_OT_hours'] <= 3,
        (timesheet_cas_OT_daily_weekly['cummulative_OT_hours'] > 3) &
        (timesheet_cas_OT_daily_weekly['prior_cummulative_OT_hours'] > 3),
        (timesheet_cas_OT_daily_weekly['cummulative_OT_hours'] > 3) &
        (timesheet_cas_OT_daily_weekly['prior_cummulative_OT_hours'] < 3)
    ],
    [
        timesheet_cas_OT_daily_weekly['cummulative_OT_hours'] - timesheet_cas_OT_daily_weekly['prior_cummulative_OT_hours'],
        0,
        3 - timesheet_cas_OT_daily_weekly['prior_cummulative_OT_hours']
    ],
    default=0
)

# Step 5: Logic for ts_ot_post_three (hours after first 3 OT hours)
timesheet_cas_OT_daily_weekly['ts_ot_post_three'] = np.where(
    (timesheet_cas_OT_daily_weekly['DOTW'] != 2) &  # Exclude Sundays
    (timesheet_cas_OT_daily_weekly['Holiday'] == 'Not a Holiday'),  # Exclude public holidays
    np.maximum(timesheet_cas_OT_daily_weekly['ot_callback_hrs'] - timesheet_cas_OT_daily_weekly['ts_ot_first_three'], 0),  # Ensure non-negative values
    0  # Default value if conditions are not met
)

# Discrepancy calculations
# Step 1: Calculate cal_balance_hours (subtract Results hours from total_hours)
timesheet_cas_OT_daily_weekly['cal_balance_hours'] = (
    timesheet_cas_OT_daily_weekly['total_hours']
    - timesheet_cas_OT_daily_weekly['cal_sunday_ot']
    - timesheet_cas_OT_daily_weekly['cal_PH_ot']
    - timesheet_cas_OT_daily_weekly['cal_first_3_ot']
    - timesheet_cas_OT_daily_weekly['cal_post_3_ot']
    - timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sat']
    - timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sun']
)


timesheet_cas_OT_daily_weekly['average_ts_loading'] = np.where(

 timesheet_cas_OT_daily_weekly['DATE WORKED'] <= pd.Timestamp('2017-05-12') 
 ,
    (
        timesheet_cas_OT_daily_weekly['CASUAL_hours'] * 1.00 +
        timesheet_cas_OT_daily_weekly['SHIFT100_hours'] * 2.00 +
        timesheet_cas_OT_daily_weekly['SHIFT50_hours'] * 1.50 +
        timesheet_cas_OT_daily_weekly['SHIFT150_hours'] * 2.50 +
        timesheet_cas_OT_daily_weekly['SATCASUAL_hours'] * 1.20 +
        timesheet_cas_OT_daily_weekly['SUNCASUAL_hours'] * 1.50 +
        timesheet_cas_OT_daily_weekly['ADDIT_hours'] * 1.00 +
        timesheet_cas_OT_daily_weekly['SHIFT15_hours'] * 1.15 +
        timesheet_cas_OT_daily_weekly['ts_ot_ph'] * 2.50 +
        timesheet_cas_OT_daily_weekly['ts_ot_sunday'] * 2.00 +
        timesheet_cas_OT_daily_weekly['ts_ot_first_three'] * 1.50 +
        timesheet_cas_OT_daily_weekly['ts_ot_post_three'] * 2.00
    ) / timesheet_cas_OT_daily_weekly['total_hours'],
    (
        timesheet_cas_OT_daily_weekly['CASUAL_hours'] * 1.00 +
        timesheet_cas_OT_daily_weekly['SHIFT100_hours'] * 2.00 +
        timesheet_cas_OT_daily_weekly['SHIFT50_hours'] * 1.50 +
        timesheet_cas_OT_daily_weekly['SHIFT150_hours'] * 2.50 +
        timesheet_cas_OT_daily_weekly['SATCASUAL_hours'] * 1.20 +
        timesheet_cas_OT_daily_weekly['SUNCASUAL_hours'] * 1.50 +
        timesheet_cas_OT_daily_weekly['ADDIT_hours'] * 1.00 +
        timesheet_cas_OT_daily_weekly['SHIFT15_hours'] * 1.15 +
        timesheet_cas_OT_daily_weekly['ts_ot_ph'] / 1.25 * 2.50 +
        timesheet_cas_OT_daily_weekly['ts_ot_sunday'] / 1.25 * 2.00 +
        timesheet_cas_OT_daily_weekly['ts_ot_first_three'] / 1.25 * 1.50 +
        timesheet_cas_OT_daily_weekly['ts_ot_post_three'] / 1.25 * 2.00
    ) / timesheet_cas_OT_daily_weekly['total_hours']
)




# Step 4: Calculate cal_factor_excl 
timesheet_cas_OT_daily_weekly['avg_cal_loading'] = np.where((
    timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sun'] * 1.5 +
    timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sat'] * 1.2 
    ) > 0,
    ((timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sun'] * 1.5 +
    timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sat'] * 1.2)
    / (timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sun'] +
    timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sat']  
     )),
     0
)



# Step 8: Calculate factor_difference_excl
# timesheet_cas_OT_daily_weekly['loading_difference_excl'] = np.minimum(
#     timesheet_cas_OT_daily_weekly['avg_cal_loading'] - timesheet_cas_OT_daily_weekly['average_ts_loading'], 0 
# )

# Amended to maximum to ensure negative values are not considered
# Step 8: Calculate factor_difference_excl
timesheet_cas_OT_daily_weekly['loading_difference_excl'] = np.maximum(
    timesheet_cas_OT_daily_weekly['avg_cal_loading'] - timesheet_cas_OT_daily_weekly['average_ts_loading'], 0 
)


# timesheet_cas_OT_daily_weekly['loading_difference_excl'] = np.minimum(
#     timesheet_cas_OT_daily_weekly['avg_cal_loading'] - timesheet_cas_OT_daily_weekly['average_ts_loading'], 0 
# )


# Step 9: Calculate discrepancy_amount_excl
timesheet_cas_OT_daily_weekly['wknd_discrepancy_amount_excl'] = (
    timesheet_cas_OT_daily_weekly['base_rate'] * timesheet_cas_OT_daily_weekly['loading_difference_excl'] 
    * (timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sun'] +
    timesheet_cas_OT_daily_weekly['cal_wknd_penalty_sat'])
    )








# # Step 9: Calculate discrepancy_amount_excl
# timesheet_cas_OT_daily_weekly['discrepancy_amount_excl'] = (
#     -timesheet_cas_OT_daily_weekly['base_rate'] * timesheet_cas_OT_daily_weekly['loading_difference_excl'] 
#     * (timesheet_cas_OT_daily_weekly['cal_sunday_ot'] +
#     timesheet_cas_OT_daily_weekly['cal_PH_ot']  +
#     timesheet_cas_OT_daily_weekly['cal_first_3_ot']  +
#     timesheet_cas_OT_daily_weekly['cal_post_3_ot']  )
# )





# me5b added here for simplicity

loading_factor = 1 - (1 / 1.25)



# Convert cutoff_date to datetime.date
cutoff_date = pd.to_datetime('2024-06-30').date()


# Apply the conditions
timesheet_cas_OT_daily_weekly.loc[timesheet_cas_OT_daily_weekly['date_only'] <= cutoff_date, 'OT_Cas_Loading_Discrp'] = (
    (timesheet_cas_OT_daily_weekly['cal_PH_ot'] * 2.50) +
    (timesheet_cas_OT_daily_weekly['cal_sunday_ot'] * 2.00) +
    (timesheet_cas_OT_daily_weekly['cal_first_3_ot'] * 1.50) +
    (timesheet_cas_OT_daily_weekly['cal_post_3_ot'] * 2.00)
) * timesheet_cas_OT_daily_weekly['base_rate'] * loading_factor

timesheet_cas_OT_daily_weekly.loc[timesheet_cas_OT_daily_weekly['date_only'] > cutoff_date, 'OT_Cas_Loading_Discrp'] = (
    (timesheet_cas_OT_daily_weekly['ts_ot_ph'] * 2.50) +
    (timesheet_cas_OT_daily_weekly['ts_ot_sunday'] * 2.00) +
    (timesheet_cas_OT_daily_weekly['ts_ot_first_three'] * 1.50) +
    (timesheet_cas_OT_daily_weekly['ts_ot_post_three'] * 2.00)
) * timesheet_cas_OT_daily_weekly['base_rate'] * loading_factor

print("Test to see what columns remain after calculations")
print(list(timesheet_cas_OT_daily_weekly.columns))


print("Shift top-up calculations complete.")

# Step 10: Save the final data to Parquet
output_parquet_file = os.path.join(output_cleaned_data, 'timesheet_cas_OT_daily_weekly.parquet')
timesheet_cas_OT_daily_weekly.to_parquet(output_parquet_file, index=False)

output_csv_file = os.path.join(output_cleaned_data, 'timesheet_cas_OT_daily_weeklyv0.2.csv')
timesheet_cas_OT_daily_weekly.to_csv(output_csv_file, index=False)

print("Daily and Weekly overtime calculations completed and saved successfully.")
# Print the length of the DataFrame
print(f"Total number of rows in the DataFrame: {len(timesheet_cas_OT_daily_weekly)}")
