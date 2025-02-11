import pandas as pd
import numpy as np
import os

# Define file paths
timesheet_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_with_student_and_perm_indicator.parquet'
output_cleaned_data = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\\'
output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'

# Load the timesheet data
timesheet_min_top_up_cals = pd.read_parquet(timesheet_path)

# Step 1: Create the "minimum_hours" column
timesheet_min_top_up_cals['minimum_hours'] = np.where(
    timesheet_min_top_up_cals['POSITION_NBR'] == '1085',
    2,  # If POSITION_NBR == '1085', set minimum_hours to 2
    np.where(
        (timesheet_min_top_up_cals['is_student'] | timesheet_min_top_up_cals['is_perm']),  # If either is_student or is_perm is True
        1,  # Set minimum_hours to 1
        3  # Otherwise, set minimum_hours to 3
    )
)

# # Output a sample of minimum_hours for testing
# sample_minimum_hours = timesheet_min_top_up_cals[['EMPLID', 'POSITION_NBR', 'is_student', 'is_perm', 'minimum_hours']].head(200)
# sample_minimum_hours_output = os.path.join(output_tests, 'sample_minimum_hours.xlsx')
# sample_minimum_hours.to_excel(sample_minimum_hours_output, index=False)
# print(f"Step 1 completed: 'minimum_hours' sample saved to {sample_minimum_hours_output}")

# Step 2: Calculate the gap between shifts (gap_hours) only when Start_null or End_null is FALSE
# Get the previous shift's end time within the same EMPLID and date_only
timesheet_min_top_up_cals['gap_hours'] = timesheet_min_top_up_cals.groupby(['EMPLID'])['datetime_endwork'].shift(1)

# Only calculate gaps where:
# 1. Both the current and previous rows have valid start and end times (Start_null == False, End_null == False)
# 2. The EMPLID and date_only are the same between the current and previous rows
timesheet_min_top_up_cals['gap_hours'] = np.where(
    (timesheet_min_top_up_cals['Start_null'] == False) &
    (timesheet_min_top_up_cals['End_null'].shift(1) == False) &
    (timesheet_min_top_up_cals['EMPLID'] == timesheet_min_top_up_cals['EMPLID'].shift(1)),  # Ensure same EMPLID
    (timesheet_min_top_up_cals['datetime_startwork'] - timesheet_min_top_up_cals['datetime_endwork'].shift(1)).dt.total_seconds() / 3600,
    np.nan  # Set gap to NaN where we don't have valid start or end times or EMPLID/date_only changes
)



# Output a sample of gap_hours for testing
sample_gap_hours = timesheet_min_top_up_cals[['EMPLID', 'date_only', 'datetime_startwork', 'datetime_endwork', 'gap_hours']].head(200)
sample_gap_hours_output = os.path.join(output_tests, 'sample_gap_hours.xlsx')
sample_gap_hours.to_excel(sample_gap_hours_output, index=False)
print(f"Step 2 completed: 'gap_hours' sample saved to {sample_gap_hours_output}")


# Step 3: Create a conditional cumulative sum of total_hours, resetting when gap_hours > 0
def conditional_cumsum(df):
    # Create a column for cumulative sum
    cumsum = []
    running_sum = 0

    for i, row in df.iterrows():
        # Reset cumulative sum if gap_hours is greater than 0 or NaN (i.e. first shift of the day)
        if row['gap_hours'] > 0 or pd.isna(row['gap_hours']):
            running_sum = row['total_hours']
        else:
            running_sum += row['total_hours']

        cumsum.append(running_sum)

    return pd.Series(cumsum, index=df.index)


# Apply the function to each group (by EMPLID and date_only)
timesheet_min_top_up_cals['conseq_cumul_sumhrs'] = timesheet_min_top_up_cals.groupby(['EMPLID', 'date_only']).apply(
    conditional_cumsum).reset_index(drop=True)

# Output a sample to verify the results
sample_cumul_sumhrs_output = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\sample_conseq_cumul_sumhrs.xlsx'
timesheet_min_top_up_cals[['EMPLID', 'date_only', 'total_hours', 'gap_hours', 'conseq_cumul_sumhrs']].head(
    200).to_excel(sample_cumul_sumhrs_output, index=False)
print(f"'conseq_cumul_sumhrs' sample saved to {sample_cumul_sumhrs_output}")

# Step 4: Create EMPLID_date_only key
timesheet_min_top_up_cals['EMPLID_date_only'] = timesheet_min_top_up_cals['EMPLID'].astype(str) + '_' + timesheet_min_top_up_cals['date_only'].astype(str)

# Step 5: Identify shifts with cumulative sum >= 3 hours
emplid_date_only_with_3hrs = timesheet_min_top_up_cals[timesheet_min_top_up_cals['conseq_cumul_sumhrs'] >= 3]['EMPLID_date_only'].unique()

# Step 6: Flag those shifts with ex_1_3hrs_day if in the list of EMPLID_date_only_with_3hrs
timesheet_min_top_up_cals['ex_1_3hrs_day'] = timesheet_min_top_up_cals['EMPLID_date_only'].isin(emplid_date_only_with_3hrs)

# Output a sample of EMPLID_date_only and ex_1_3hrs_day for testing
sample_3hrs_day = timesheet_min_top_up_cals[['EMPLID', 'date_only', 'EMPLID_date_only', 'conseq_cumul_sumhrs', 'ex_1_3hrs_day']].head(200)
sample_3hrs_day_output = os.path.join(output_tests, 'sample_ex_1_3hrs_day.xlsx')
sample_3hrs_day.to_excel(sample_3hrs_day_output, index=False)
print(f"Step 5 completed: 'ex_1_3hrs_day' sample saved to {sample_3hrs_day_output}")

# Step 6: Calculate gap_hours_sub for the next shift, ensuring no calculation if Start_null or End_null is TRUE
# Get the next shift's start time within the same EMPLID and date_only
timesheet_min_top_up_cals['gap_hours_sub'] = timesheet_min_top_up_cals.groupby(['EMPLID', 'date_only'])['datetime_startwork'].shift(-1)

# Only calculate gaps where:
# 1. Both the current and next rows have valid start and end times (Start_null == False, End_null == False)
# 2. The EMPLID and date_only are the same between the current and next rows
timesheet_min_top_up_cals['gap_hours_sub'] = np.where(
    (timesheet_min_top_up_cals['Start_null'] == False) &
    (timesheet_min_top_up_cals['End_null'].shift(-1) == False) &
    (timesheet_min_top_up_cals['EMPLID'] == timesheet_min_top_up_cals['EMPLID'].shift(-1)) &  # Ensure same EMPLID
    (timesheet_min_top_up_cals['date_only'] == timesheet_min_top_up_cals['date_only'].shift(-1)),  # Ensure same date_only
    (timesheet_min_top_up_cals['datetime_startwork'].shift(-1) - timesheet_min_top_up_cals['datetime_endwork']).dt.total_seconds() / 3600,
    np.nan  # Set gap to NaN where we don't have valid start or end times or EMPLID/date_only changes
)

# Fill NaNs with 24-hour gaps (for cases with no next shift or invalid data)
timesheet_min_top_up_cals['gap_hours_sub'] = timesheet_min_top_up_cals['gap_hours_sub'].fillna(24)

# Output a sample of gap_hours_sub for testing
sample_gap_hours_sub = timesheet_min_top_up_cals[['EMPLID', 'date_only', 'datetime_startwork', 'datetime_endwork', 'gap_hours_sub']].head(200)
sample_gap_hours_sub_output = os.path.join(output_tests, 'sample_gap_hours_sub.xlsx')
sample_gap_hours_sub.to_excel(sample_gap_hours_sub_output, index=False)
print(f"Step 6 completed: 'gap_hours_sub' sample saved to {sample_gap_hours_sub_output}")

# Step 7: Calculate cumul_gaphrs (conditional cumulative sum of gap_hours_sub)
timesheet_min_top_up_cals['cumul_gaphrs'] = timesheet_min_top_up_cals.groupby(['EMPLID', 'date_only'])['gap_hours_sub'].transform(
    lambda x: x.cumsum()
)

# Output a sample of cumul_gaphrs for testing
sample_cumul_gaphrs = timesheet_min_top_up_cals[['EMPLID', 'date_only', 'gap_hours_sub', 'cumul_gaphrs']].head(200)
sample_cumul_gaphrs_output = os.path.join(output_tests, 'sample_cumul_gaphrs.xlsx')
sample_cumul_gaphrs.to_excel(sample_cumul_gaphrs_output, index=False)
print(f"Step 7 completed: 'cumul_gaphrs' sample saved to {sample_cumul_gaphrs_output}")

# Step 8: Calculate elapsed_hrs (gap adjusted hours)
timesheet_min_top_up_cals['elapsed_hrs'] = np.where(
    timesheet_min_top_up_cals['EMPLID_date_only'] != timesheet_min_top_up_cals['EMPLID_date_only'].shift(1),
    timesheet_min_top_up_cals['total_hours'],  # If new EMPLID_date_only, just take total_hours
    timesheet_min_top_up_cals['daily_hrs_count'] + timesheet_min_top_up_cals['cumul_gaphrs'].shift(1)  # Otherwise, add cumulative gap hours
)

# Output a sample of elapsed_hrs for testing
sample_elapsed_hrs = timesheet_min_top_up_cals[['EMPLID', 'date_only', 'total_hours', 'cumul_gaphrs', 'elapsed_hrs']].head(200)
sample_elapsed_hrs_output = os.path.join(output_tests, 'sample_elapsed_hrs.xlsx')
sample_elapsed_hrs.to_excel(sample_elapsed_hrs_output, index=False)
print(f"Step 8 completed: 'elapsed_hrs' sample saved to {sample_elapsed_hrs_output}")

# Step 9: Calculate three_hour_top_up
timesheet_min_top_up_cals['three_hour_top_up'] = np.where(
    timesheet_min_top_up_cals['elapsed_hrs'] >= 3, 0,  # If elapsed hours >= 3, no top-up needed
    np.where(
        timesheet_min_top_up_cals['elapsed_hrs'] + timesheet_min_top_up_cals['gap_hours_sub'] <= 3,
        timesheet_min_top_up_cals['gap_hours_sub'],  # Top-up the entire gap if it stays within 3 hours
        3 - timesheet_min_top_up_cals['elapsed_hrs']  # Otherwise, top-up to exactly 3 hours
    )
)

# Output a sample of three_hour_top_up for testing
sample_three_hour_top_up = timesheet_min_top_up_cals[['EMPLID', 'date_only', 'elapsed_hrs', 'gap_hours_sub', 'three_hour_top_up']].head(200)
sample_three_hour_top_up_output = os.path.join(output_tests, 'sample_three_hour_top_up.xlsx')
sample_three_hour_top_up.to_excel(sample_three_hour_top_up_output, index=False)
print(f"Step 9 completed: 'three_hour_top_up' sample saved to {sample_three_hour_top_up_output}")

# Ensure that the top-up columns are initialized as float
timesheet_min_top_up_cals['one_hour_top_up'] = 0.0  # Explicitly cast as float
timesheet_min_top_up_cals['two_hour_top_up'] = 0.0  # Explicitly cast as float

# Step 9: Calculate two_hour_top_up (similar to three_hour_top_up)
timesheet_min_top_up_cals['two_hour_top_up'] = np.where(
    (timesheet_min_top_up_cals['POSITION_NBR'] == '1085') &  # Apply the rule only for POSITION_NBR == '1085'
    (timesheet_min_top_up_cals['elapsed_hrs'] < 2),  # If elapsed hours are less than 2
    np.where(
        timesheet_min_top_up_cals['elapsed_hrs'] + timesheet_min_top_up_cals['gap_hours_sub'] <= 2,
        timesheet_min_top_up_cals['gap_hours_sub'],  # Top-up the entire gap if it stays within 2 hours
        2 - timesheet_min_top_up_cals['elapsed_hrs']  # Otherwise, top-up to exactly 2 hours
    ),
    0  # No top-up if elapsed hours are already 2 or more
)

# Output sample after applying Two-Hour Top-Up
two_hour_sample_output_file = os.path.join(output_tests, 'two_hour_top_up_sample.xlsx')
timesheet_min_top_up_cals[timesheet_min_top_up_cals['POSITION_NBR'] == '1085'][['EMPLID', 'date_only', 'datetime_startwork', 'elapsed_hrs', 'gap_hours_sub', 'two_hour_top_up']].head(2000).to_excel(two_hour_sample_output_file, index=False)
print(f"Two-Hour Top-Up sample saved to: {two_hour_sample_output_file}")

# One-Hour Top-Up Logic (adjust gap_hours_sub with null checks)
def calculate_one_hour_top_up(df):
    df = df.sort_values(by=['EMPLID', 'date_only', 'datetime_startwork'])
    df['one_hour_min_elapsed_hrs'] = df['elapsed_hrs']

    for i in range(len(df) - 1):
        current_row = df.iloc[i]
        next_row = df.iloc[i + 1]

        # Use a new gap_hours for this top-up calculation that excludes invalid rows
        gap_hours_sub = (next_row['datetime_startwork'] - current_row['datetime_startwork']).total_seconds() / 3600

        if (current_row['Start_null'] or next_row['Start_null'] or current_row['End_null'] or next_row['End_null']):
            continue  # Skip any rows with invalid data

        if current_row['one_hour_min_elapsed_hrs'] >= 1:
            continue

        elif current_row['one_hour_min_elapsed_hrs'] + gap_hours_sub <= 1:
            df.at[i, 'one_hour_top_up'] = gap_hours_sub
            df.at[i, 'one_hour_min_elapsed_hrs'] += gap_hours_sub

        else:
            df.at[i, 'one_hour_top_up'] = 1 - current_row['one_hour_min_elapsed_hrs']
            df.at[i, 'one_hour_min_elapsed_hrs'] = 1

    return df




# Step 10: Apply the One-Hour Top-Up Rule
timesheet_min_top_up_cals['one_hour_top_up'] = 0  # Initialize the field
timesheet_min_top_up_cals = calculate_one_hour_top_up(timesheet_min_top_up_cals)

# Output sample after applying One-Hour Top-Up
one_hour_sample_output_file = os.path.join(output_tests, 'one_hour_top_up_sample.xlsx')
timesheet_min_top_up_cals[['EMPLID', 'date_only', 'datetime_startwork', 'elapsed_hrs', 'one_hour_top_up']].head(2000).to_excel(one_hour_sample_output_file, index=False)
print(f"One-Hour Top-Up sample saved to: {one_hour_sample_output_file}")

import pandas as pd
import numpy as np


# Step 10: Calculate loading based on the conditions for OT and non-OT for each top-up field
def calculate_loading(row, top_up_field):
    if row['PIN_NM'] != 'OT':
        # Non-OT: loading is based on ts_factor / total_hours
        return row['ts_factor'] / row['total_hours'] if row['total_hours'] > 0 else 1.0
    else:
        # OT rules
        if row['ts_ot_ph'] > 0:
            return 2.5  # 250% loading for public holidays
        elif row['ts_ot_sunday'] > 0:
            return 2.0  # 200% loading for Sundays
        elif row['ts_ot_post_three'] > 0:
            return 2.0  # 200% loading for post-three OT hours

        # Handle the specific top-up field (three_hour_top_up, one_hour_top_up, or two_hour_top_up)
        elif (row[top_up_field] + row['ts_ot_first_three']) < 3:
            return 1.5  # 150% loading if total (top-up + ts_ot_first_three) < 3 hours
        else:
            # Mixed loading: calculate based on first 3 hours and remaining hours
            first_three_hours_loading = (3 - row['ts_ot_first_three']) * 1.5 if row['ts_ot_first_three'] < 3 else 0
            post_three_hours_loading = (row[top_up_field] - (3 - row['ts_ot_first_three'])) * 2.0 if row[top_up_field] > (3 - row['ts_ot_first_three']) else 0

            return first_three_hours_loading + post_three_hours_loading


# Step 10: Adjust base rate for OT and date conditions
def adjust_base_rate(row):
    if row['PIN_NM'] == 'OT' and row['DATE WORKED'] < pd.Timestamp('2017-05-13'):
        return row['base_rate'] / 1.25
    else:
        return row['base_rate']


# Step 11: Apply the base rate adjustment to a new column 'adjusted_base_rate'
timesheet_min_top_up_cals['adjusted_base_rate'] = timesheet_min_top_up_cals.apply(adjust_base_rate, axis=1)

# Step 12: Calculate the loading for each top-up field independently
timesheet_min_top_up_cals['three_hour_top_up_loading'] = timesheet_min_top_up_cals.apply(
    lambda row: calculate_loading(row, 'three_hour_top_up'), axis=1
)

timesheet_min_top_up_cals['one_hour_top_up_loading'] = timesheet_min_top_up_cals.apply(
    lambda row: calculate_loading(row, 'one_hour_top_up'), axis=1
)

timesheet_min_top_up_cals['two_hour_top_up_loading'] = timesheet_min_top_up_cals.apply(
    lambda row: calculate_loading(row, 'two_hour_top_up'), axis=1
)

# Step 13: Recalculate the cash values for each top-up field using the adjusted base rate and its specific loading
timesheet_min_top_up_cals['three_hour_top_up_cash'] = np.where(
    timesheet_min_top_up_cals['minimum_hours'] == 3,
    timesheet_min_top_up_cals['three_hour_top_up'] *
    timesheet_min_top_up_cals['three_hour_top_up_loading'] *
    timesheet_min_top_up_cals['adjusted_base_rate'],
    0  # Else set to 0
)

# Apply the condition: if minimum_hours is not 2, calculate one_hour_top_up_cash, else set it to 0
timesheet_min_top_up_cals['one_hour_top_up_cash'] = np.where(
    timesheet_min_top_up_cals['minimum_hours'] != 2,
    timesheet_min_top_up_cals['one_hour_top_up'] *
    timesheet_min_top_up_cals['one_hour_top_up_loading'] *
    timesheet_min_top_up_cals['adjusted_base_rate'],
    0  # Else set to 0
)

timesheet_min_top_up_cals['two_hour_top_up_cash'] = (
        timesheet_min_top_up_cals['two_hour_top_up'] *
        timesheet_min_top_up_cals['two_hour_top_up_loading'] *
        timesheet_min_top_up_cals['adjusted_base_rate']
)

# Output full sample to Excel for checking final result
final_sample_output_file = os.path.join(output_tests, 'final_timesheet_with_top_ups_sample.xlsx')
timesheet_min_top_up_cals.head(20000).to_excel(final_sample_output_file, index=False)  # Save all columns in the sample
print(f"Full sample with top-ups saved to: {final_sample_output_file}")

# Step 11: Output the final table to Parquet
timesheet_min_top_up_cals.to_parquet(output_cleaned_data + 'timesheet_min_top_up_cals.parquet', index=False)

# # Two-Hour Top-Up Logic (adjust gap_hours_sub with null checks)
# def calculate_two_hour_top_up(df):
#     df = df.sort_values(by=['EMPLID', 'date_only', 'datetime_startwork'])
#     df['two_hour_min_elapsed_hrs'] = df['elapsed_hrs'].where(df['POSITION_NBR'] == '1085', 0)
#
#     for i in range(len(df) - 1):
#         current_row = df.iloc[i]
#         next_row = df.iloc[i + 1]
#
#         if current_row['POSITION_NBR'] != '1085':
#             continue
#
#         # Use a new gap_hours_sub for this top-up calculation
#         gap_hours_sub = (next_row['datetime_startwork'] - current_row['datetime_startwork']).total_seconds() / 3600
#
#         if (current_row['Start_null'] or next_row['Start_null'] or current_row['End_null'] or next_row['End_null']):
#             continue  # Skip rows with invalid data
#
#         if current_row['two_hour_min_elapsed_hrs'] >= 2:
#             continue
#
#         elif current_row['two_hour_min_elapsed_hrs'] + gap_hours_sub <= 2:
#             df.at[i, 'two_hour_top_up'] = gap_hours_sub
#             df.at[i, 'two_hour_min_elapsed_hrs'] += gap_hours_sub
#
#         else:
#             df.at[i, 'two_hour_top_up'] = 2 - current_row['two_hour_min_elapsed_hrs']
#             df.at[i, 'two_hour_min_elapsed_hrs'] = 2
#
#     return df

# # Step 11: Apply the Two-Hour Top-Up Rule (for POSITION_NBR == '1085')
# timesheet_min_top_up_cals['two_hour_top_up'] = 0  # Initialize the field
# timesheet_min_top_up_cals = calculate_two_hour_top_up(timesheet_min_top_up_cals)
#
# # Output sample after applying Two-Hour Top-Up
# two_hour_sample_output_file = os.path.join(output_tests, 'two_hour_top_up_sample.xlsx')
# timesheet_min_top_up_cals[timesheet_min_top_up_cals['POSITION_NBR'] == '1085'][['EMPLID', 'date_only', 'datetime_startwork', 'elapsed_hrs', 'two_hour_top_up']].head(2000).to_excel(two_hour_sample_output_file, index=False)
# print(f"Two-Hour Top-Up sample saved to: {two_hour_sample_output_file}")