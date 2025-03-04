import pandas as pd
import numpy as np
import os


# Define file paths
#Paul's file paths
#timesheet_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_with_student_and_perm_indicator.parquet'
#output_cleaned_data = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\\'
#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'


#Sam's file paths

timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_with_student_and_perm_indicator.parquet"
                                                                            
#timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals_MealAllowance.parquet"

#timesheet_min_top_up_cals.parquet


output_cleaned_data = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"

output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"


# Load the timesheet data
timesheet_min_top_up_cals = pd.read_parquet(timesheet_path)



#print(timesheet_min_top_up_cals.columns)
'''
# Step 1: Create the "minimum_hours" column
timesheet_min_top_up_cals['minimum_hours'] = np.where(
    timesheet_min_top_up_cals['POSITION_NBR'] == '1085.0',
    2,  # If POSITION_NBR == '1085', set minimum_hours to 2
    np.where(
        (timesheet_min_top_up_cals['is_student'] | timesheet_min_top_up_cals['is_perm']),  # If either is_student or is_perm is True
        1,  # Set minimum_hours to 1
        3  # Otherwise, set minimum_hours to 3
    )
)
'''


# Step 1: Create the "minimum_hours" column
timesheet_min_top_up_cals['minimum_hours'] = np.where(
    (timesheet_min_top_up_cals['POSITION_NBR'] == '1085.0') & (timesheet_min_top_up_cals['Acad/Prof'] != 'ACAD'),
    2,  # If POSITION_NBR == '1085' and Acad/Prof is not 'ACAD', set minimum_hours to 2
    np.where(
        (timesheet_min_top_up_cals['Acad/Prof'] == 'ACAD') | 
        (timesheet_min_top_up_cals['is_student']) | 
        (timesheet_min_top_up_cals['is_perm']),  # If either Acad/Prof is 'ACAD', is_student is True, or is_perm is True
        1,  # Set minimum_hours to 1
        3   # Otherwise, set minimum_hours to 3
    )
)






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
    conditional_cumsum, include_groups=True).reset_index(drop=True)

# Output a sample to verify the results
#Paul's file path
#sample_cumul_sumhrs_output = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests'
# Sam's file path
sample_cumul_sumhrs_output = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\sample_conseq_cumul_sumhrs.xlsx'
timesheet_min_top_up_cals[['EMPLID', 'date_only', 'total_hours', 'gap_hours', 'conseq_cumul_sumhrs']].head(
    200).to_excel(sample_cumul_sumhrs_output, index=False)
print(f"'conseq_cumul_sumhrs' sample saved to {sample_cumul_sumhrs_output}")

# Step 4: Create EMPLID_date_only key
timesheet_min_top_up_cals['EMPLID_date_only'] = timesheet_min_top_up_cals['EMPLID'].astype(str) + '_' + timesheet_min_top_up_cals['date_only'].astype(str)

# Step 2: Identify EMPLID and date_only combinations for ex_1_3hrs_day
three_hour_shifts = []

for i in range(len(timesheet_min_top_up_cals) - 1):
    current_row = timesheet_min_top_up_cals.iloc[i]
    next_row = timesheet_min_top_up_cals.iloc[i + 1]

    # Condition 1: Shift with cumulative hours >= 3 on the same day
    if current_row['conseq_cumul_sumhrs'] >= 3:
        three_hour_shifts.append((current_row['EMPLID'], current_row['date_only']))

    # Condition 2: Midnight-end shift, with a consecutive shift that adds up to >= 3 hours
    elif (
        current_row['datetime_endwork'].time() == pd.Timestamp('00:00:00').time() and
        current_row['EMPLID'] == next_row['EMPLID'] and
        current_row['date_only'] == next_row['date_only'] and
        next_row['gap_hours'] == 0 and
        (current_row['total_hours'] + next_row['total_hours']) >= 3
    ):
        three_hour_shifts.append((current_row['EMPLID'], current_row['date_only']))

# Convert list to a set of unique (EMPLID, date_only) combinations
three_hour_shifts = set(three_hour_shifts)

# Step 3: Flag ex_1_3hrs_day based on identified (EMPLID, date_only) combinations
timesheet_min_top_up_cals['ex_1_3hrs_day'] = timesheet_min_top_up_cals.apply(
    lambda row: (row['EMPLID'], row['date_only']) in three_hour_shifts, axis=1
)

# Output a sample to verify the results
sample_3hrs_day_output = os.path.join(output_tests, 'sample_ex_1_3hrs_day_updated.xlsx')
timesheet_min_top_up_cals[['EMPLID', 'date_only', 'datetime_startwork', 'datetime_endwork', 'conseq_cumul_sumhrs', 'ex_1_3hrs_day']].head(200).to_excel(sample_3hrs_day_output, index=False)
print(f"Updated 'ex_1_3hrs_day' sample saved to {sample_3hrs_day_output}")


# Step 6: Calculate gap_hours_sub for the next shift, ensuring no calculation if Start_null or End_null is TRUE
# Get the next shift's start time within the same EMPLID and date_only
timesheet_min_top_up_cals['gap_hours_sub'] = timesheet_min_top_up_cals.groupby(['EMPLID', 'date_only'])['datetime_startwork'].shift(-1)

# Only calculate gaps where:
# 1. Both the current and next rows have valid start and end times (Start_null == False, End_null == False)
# 2. The EMPLID and date_only are the same between the current and next rows
timesheet_min_top_up_cals['gap_hours_sub'] = np.where(
    (timesheet_min_top_up_cals['Start_null'] == False) &
    (timesheet_min_top_up_cals['End_null'].shift(-1) == False) &
    (timesheet_min_top_up_cals['EMPLID'] == timesheet_min_top_up_cals['EMPLID'].shift(-1)),  # Ensure same EMPLID
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

# Step 8: Calculate the true first shift of the day based on refined criteria
# Initialize the column to mark if it's the first shift for the day
timesheet_min_top_up_cals['is_first_shift'] = False



# # Iterate through rows to determine `is_first_shift`
# for i in range(1, len(timesheet_min_top_up_cals)):
#     current_row = timesheet_min_top_up_cals.iloc[i]
#     previous_row = timesheet_min_top_up_cals.iloc[i - 1]
#
#     # Condition 1: New EMPLID marks a new shift
#     if current_row['EMPLID'] != previous_row['EMPLID']:
#         timesheet_min_top_up_cals.at[i, 'is_first_shift'] = True
#         continue
#
#     # Condition 2: New date, but not a midnight straddle
#     if current_row['date_only'] != previous_row['date_only']:
#         # Check for a midnight straddle (00:00:00) between consecutive dates
#         if not (
#             current_row['datetime_startwork'].time() == pd.Timestamp('00:00:00').time() and
#             previous_row['datetime_endwork'].time() == pd.Timestamp('00:00:00').time()
#         ):
#             timesheet_min_top_up_cals.at[i, 'is_first_shift'] = True
#             continue
#
#         # Condition 3: Midnight carryover with consistent EMPLID (find first prior row where gap_hours > 0)
#     if (
#             current_row['datetime_startwork'].time() == pd.Timestamp('00:00:00').time() and
#             previous_row['datetime_endwork'].time() == pd.Timestamp('00:00:00').time() and
#             current_row['EMPLID'] == previous_row['EMPLID']
#     ):
#         # Find the first prior row where gap_hours is non-zero, with same EMPLID
#         j = i - 1
#         while j >= 0:
#             check_row = timesheet_min_top_up_cals.iloc[j]
#             if check_row['EMPLID'] != current_row['EMPLID']:
#                 break  # Stop if EMPLID changes in prior rows
#             if check_row['gap_hours'] > 0:
#                 # Mark the next row after this as the true first shift for this sequence
#                 timesheet_min_top_up_cals.at[j, 'is_first_shift'] = True
#                 break
#             j -= 1



# Initialize columns to mark which conditions are satisfied for each row
timesheet_min_top_up_cals['is_first_shift'] = False
timesheet_min_top_up_cals['cond_new_emplid'] = False
timesheet_min_top_up_cals['cond_new_date'] = False
timesheet_min_top_up_cals['cond_midnight_straddle'] = False
timesheet_min_top_up_cals['cond_midnight_carryover'] = False
timesheet_min_top_up_cals['first_prior_gap_found'] = False

# Iterate through rows to determine `is_first_shift`
for i in range(1, len(timesheet_min_top_up_cals)):
    current_row = timesheet_min_top_up_cals.iloc[i]
    previous_row = timesheet_min_top_up_cals.iloc[i - 1]

    # Condition 1: New EMPLID marks a new shift
    if current_row['EMPLID'] != previous_row['EMPLID']:
        timesheet_min_top_up_cals.at[i, 'is_first_shift'] = True
        timesheet_min_top_up_cals.at[i, 'cond_new_emplid'] = True
        continue

    # Condition 2: New date, but not a midnight straddle
    if current_row['date_only'] != previous_row['date_only']:
        # Check if the date change is due to a midnight straddle
        if (
            current_row['datetime_startwork'].time() == pd.Timestamp('00:00:00').time() and
            previous_row['datetime_endwork'].time() == pd.Timestamp('00:00:00').time()
        ):
            timesheet_min_top_up_cals.at[i, 'cond_midnight_straddle'] = True
        else:
            # If it's not a midnight straddle, mark `cond_new_date` as True and `is_first_shift`
            timesheet_min_top_up_cals.at[i, 'cond_new_date'] = True
            timesheet_min_top_up_cals.at[i, 'is_first_shift'] = True
            continue

    # Condition 3: Midnight carryover with consistent EMPLID (find first prior row where gap_hours > 0)
    if (
        current_row['datetime_startwork'].time() == pd.Timestamp('00:00:00').time() and
        previous_row['datetime_endwork'].time() == pd.Timestamp('00:00:00').time() and
        current_row['EMPLID'] == previous_row['EMPLID']
    ):
        timesheet_min_top_up_cals.at[i, 'cond_midnight_carryover'] = True
        # Backtrack to find the first prior row where gap_hours is non-zero, with the same EMPLID
        j = i - 1
        while j >= 0:
            check_row = timesheet_min_top_up_cals.iloc[j]
            if check_row['EMPLID'] != current_row['EMPLID']:
                break  # Stop if EMPLID changes in prior rows
            if check_row['gap_hours'] > 0:
                # Mark this row as the true first shift in this midnight sequence
                timesheet_min_top_up_cals.at[j, 'is_first_shift'] = True
                timesheet_min_top_up_cals.at[j, 'first_prior_gap_found'] = True
                break
            j -= 1

# Output a sample to verify `is_first_shift`
sample_first_shift_output = os.path.join(output_tests, 'sample_is_first_shift_adjusted.xlsx')
timesheet_min_top_up_cals[['EMPLID', 'date_only', 'datetime_startwork', 'datetime_endwork', 'gap_hours', 'is_first_shift']].head(200).to_excel(sample_first_shift_output, index=False)
print(f"Updated 'is_first_shift' with refined criteria saved to {sample_first_shift_output}")

# Step 9: Calculate elapsed hours with reset based on `is_first_shift`, including gap_hours
timesheet_min_top_up_cals['elapsed_hrs'] = 0

# Iterate over rows to calculate elapsed hours
for i in range(len(timesheet_min_top_up_cals)):
    current_row = timesheet_min_top_up_cals.iloc[i]

    # For the first row, set elapsed hours to total_hours as there's no prior row
    if i == 0:
        timesheet_min_top_up_cals.at[i, 'elapsed_hrs'] = current_row['total_hours']
        continue

    # Reference the previous row for accumulation
    previous_row = timesheet_min_top_up_cals.iloc[i - 1]

    # Reset `elapsed_hrs` if it's the first shift of the day
    if current_row['is_first_shift']:
        timesheet_min_top_up_cals.at[i, 'elapsed_hrs'] = current_row['total_hours']
    else:
        # Accumulate elapsed hours for continuous shifts within the same employee and day, including gap_hours
        timesheet_min_top_up_cals.at[i, 'elapsed_hrs'] = (
            previous_row['elapsed_hrs'] + current_row['total_hours'] + current_row['gap_hours']
        )

# Output a sample to verify the `elapsed_hrs` calculation
sample_elapsed_hrs_output = os.path.join(output_tests, 'sample_elapsed_hrs_with_gap.xlsx')
timesheet_min_top_up_cals[['EMPLID', 'date_only', 'datetime_startwork', 'datetime_endwork', 'gap_hours', 'is_first_shift', 'elapsed_hrs']].head(200).to_excel(sample_elapsed_hrs_output, index=False)
print(f"Sample with 'elapsed_hrs' calculation including gap_hours saved to {sample_elapsed_hrs_output}")

'''

# Step 9: Calculate three_hour_top_up
timesheet_min_top_up_cals['three_hour_top_up'] = np.where(
    timesheet_min_top_up_cals['elapsed_hrs'] >= 3 or timesheet_min_top_up_cals['ex_1_3hrs_day'] == True, 0,  # If elapsed hours >= 3, no top-up needed
    np.where(
        timesheet_min_top_up_cals['elapsed_hrs'] + timesheet_min_top_up_cals['gap_hours_sub'] <= 3,
        timesheet_min_top_up_cals['gap_hours_sub'],  # Top-up the entire gap if it stays within 3 hours
        3 - timesheet_min_top_up_cals['elapsed_hrs']  # Otherwise, top-up to exactly 3 hours
    )
)

'''


# Clean and trim the POSITION_NBR column
timesheet_min_top_up_cals['POSITION_NBR'] = (
    timesheet_min_top_up_cals['POSITION_NBR']
    .astype(str)                         # Ensure all values are strings
    .str.strip()                         # Remove leading and trailing whitespace
    .str.upper()                         # Convert to uppercase (if needed for consistency)
    .str.replace(r'\s+', '', regex=True) # Remove internal spaces (if necessary)
)






#New code as part of Query DQ16 included condition to prevent the two_hour top post Jan 1st 2023
# Step 9: Calculate three_hour_top_up

'''
timesheet_min_top_up_cals['three_hour_top_up'] = np.where(
    (timesheet_min_top_up_cals['elapsed_hrs'] >= 3) | 
    (timesheet_min_top_up_cals['ex_1_3hrs_day'] == True) | 
    (timesheet_min_top_up_cals['DATE WORKED'] > pd.Timestamp('2023-01-01')),  # Exclude dates after 01/01/2023
    0,  # No top-up needed
    np.where(
        timesheet_min_top_up_cals['elapsed_hrs'] + timesheet_min_top_up_cals['gap_hours_sub'] <= 3,
        timesheet_min_top_up_cals['gap_hours_sub'],  # Top-up the entire gap if it stays within 3 hours
        3 - timesheet_min_top_up_cals['elapsed_hrs']  # Otherwise, top-up to exactly 3 hours
    )
)

'''
timesheet_min_top_up_cals['three_hour_top_up'] = np.where(
    (timesheet_min_top_up_cals['elapsed_hrs'] >= 3) | 
    (timesheet_min_top_up_cals['ex_1_3hrs_day'] == True) | 
    (timesheet_min_top_up_cals['DATE WORKED'] >= pd.Timestamp('2023-01-01')),  # Exclude dates on or after 01/01/2023
    0,  # No top-up needed
    np.where(
        timesheet_min_top_up_cals['elapsed_hrs'] + timesheet_min_top_up_cals['gap_hours_sub'] <= 3,
        timesheet_min_top_up_cals['gap_hours_sub'],  # Top-up the entire gap if it stays within 3 hours
        3 - timesheet_min_top_up_cals['elapsed_hrs']  # Otherwise, top-up to exactly 3 hours
    )
)


#add condition where the column is true x 1-3 hours days is true 

# Output a sample of three_hour_top_up for testing
sample_three_hour_top_up = timesheet_min_top_up_cals[['EMPLID', 'date_only', 'elapsed_hrs', 'gap_hours_sub', 'three_hour_top_up']].head(200)
sample_three_hour_top_up_output = os.path.join(output_tests, 'sample_three_hour_top_up.xlsx')
sample_three_hour_top_up.to_excel(sample_three_hour_top_up_output, index=False)
print(f"Step 9 completed: 'three_hour_top_up' sample saved to {sample_three_hour_top_up_output}")

# Ensure that the top-up columns are initialized as float
timesheet_min_top_up_cals['one_hour_top_up'] = 0.0  # Explicitly cast as float
timesheet_min_top_up_cals['two_hour_top_up'] = 0.0  # Explicitly cast as float

'''
# Step 9: Calculate two_hour_top_up (similar to three_hour_top_up)
timesheet_min_top_up_cals['two_hour_top_up'] = np.where(
    (timesheet_min_top_up_cals['POSITION_NBR'] == '1085') &  # Apply the rule only for POSITION_NBR == '1085'
    (timesheet_min_top_up_cals['POSITION_NBR'] == '1085') &  # Apply the rule only for POSITION_NBR == '1085'
    (timesheet_min_top_up_cals['elapsed_hrs'] < 2) & (timesheet_min_top_up_cals['ex_1_3hrs_day'] == False),  # If elapsed hours are less than 2
    np.where(
        timesheet_min_top_up_cals['elapsed_hrs'] + timesheet_min_top_up_cals['gap_hours_sub'] <= 2,
        timesheet_min_top_up_cals['gap_hours_sub'],  # Top-up the entire gap if it stays within 2 hours
        2 - timesheet_min_top_up_cals['elapsed_hrs']  # Otherwise, top-up to exactly 2 hours
    ),
    0  # No top-up if elapsed hours are already 2 or more
)

'''

# Check the cleaned column
print(timesheet_min_top_up_cals['POSITION_NBR'].unique())


#New code as part of Query DQ16 included condition to prevent the two_hour top post Jan 1st 2023
# Step 9: Calculate two_hour_top_up (similar to three_hour_top_up)
timesheet_min_top_up_cals['two_hour_top_up'] = np.where(
    (timesheet_min_top_up_cals['POSITION_NBR'] == '1085.0')&  # Apply the rule only for POSITION_NBR == '1085'
    (timesheet_min_top_up_cals['elapsed_hrs'] < 2) &  # If elapsed hours are less than 2
    (timesheet_min_top_up_cals['ex_1_3hrs_day'] == False) &  # Additional condition
    (timesheet_min_top_up_cals['DATE WORKED'] <= pd.Timestamp('2023-01-01')),  # Ensure DATE_WORKED is on or before 01/01/2023
    np.where(
        timesheet_min_top_up_cals['elapsed_hrs'] + timesheet_min_top_up_cals['gap_hours_sub'] <= 2,
        timesheet_min_top_up_cals['gap_hours_sub'],  # Top-up the entire gap if it stays within 2 hours
        2 - timesheet_min_top_up_cals['elapsed_hrs']  # Otherwise, top-up to exactly 2 hours
    ),
    0  # Default value if the conditions are not met
)



# Test condition 1: POSITION_NBR == '1085'
timesheet_min_top_up_cals['condition_1_position_nbr'] = timesheet_min_top_up_cals['POSITION_NBR'] == '1085.0'

# Test condition 2: elapsed_hrs < 2
timesheet_min_top_up_cals['condition_2_elapsed_hrs'] = timesheet_min_top_up_cals['elapsed_hrs'] < 2

# Test condition 3: ex_1_3hrs_day == False
timesheet_min_top_up_cals['condition_3_ex_1_3hrs_day'] = timesheet_min_top_up_cals['ex_1_3hrs_day'] == False

# Test condition 4: DATE_WORKED <= 2023-01-01
timesheet_min_top_up_cals['condition_4_date_worked'] = timesheet_min_top_up_cals['DATE WORKED'] <= pd.Timestamp('2023-01-01')

# Test combined condition for gap_hours_sub + elapsed_hrs <= 2
timesheet_min_top_up_cals['condition_5_gap_and_elapsed'] = (
    timesheet_min_top_up_cals['elapsed_hrs'] + timesheet_min_top_up_cals['gap_hours_sub'] <= 2
)

# Inspect the DataFrame to see which conditions are met
print(timesheet_min_top_up_cals[
    ['condition_1_position_nbr', 
     'condition_2_elapsed_hrs', 
     'condition_3_ex_1_3hrs_day', 
     'condition_4_date_worked', 
     'condition_5_gap_and_elapsed']
])


two_hour_topup_ConditionTest = timesheet_min_top_up_cals[
    ['condition_1_position_nbr', 
     'condition_2_elapsed_hrs', 
     'condition_3_ex_1_3hrs_day', 
     'condition_4_date_worked', 
     'condition_5_gap_and_elapsed']]

two_hour_topup_ConditionTest.to_csv('two_hour_topup_ConditionTest.csv')




# Output sample after applying Two-Hour Top-Up
two_hour_sample_output_file = os.path.join(output_tests, 'two_hour_top_up_sample.xlsx')
timesheet_min_top_up_cals[timesheet_min_top_up_cals['POSITION_NBR'] == '1085.0'][['EMPLID', 'date_only', 'datetime_startwork', 'elapsed_hrs', 'gap_hours_sub', 'two_hour_top_up']].head(2000).to_excel(two_hour_sample_output_file, index=False)
print(f"Two-Hour Top-Up sample saved to: {two_hour_sample_output_file}")

# Step 6: One-Hour Top-Up Logic with resetting based on gap_hours_sub > 1 hour and EMPLID change
def calculate_one_hour_top_up(df):
    # Sort by EMPLID and datetime_startwork to ensure proper ordering
    df = df.sort_values(by=['EMPLID', 'datetime_startwork']).copy()

    # Initialize the one-hour fields
    df['one_hour_min_elapsed_hrs'] = df['total_hours']  # Start with total_hours
    df['one_hour_top_up'] = 0  # Initialize one_hour_top_up as 0

    for i in range(len(df) - 1):
        current_row = df.iloc[i]
        next_row = df.iloc[i + 1]

        # Reset condition: New EMPLID or gap_hours_sub + total_hours > 1 hour
        if current_row['EMPLID'] != next_row['EMPLID'] or (current_row['gap_hours_sub'] + current_row['total_hours'] > 1):
            # Reset one_hour_min_elapsed_hrs for the next row
            df.at[i + 1, 'one_hour_min_elapsed_hrs'] = next_row['total_hours']
        else:
            # Accumulate hours if reset condition not met
            df.at[i + 1, 'one_hour_min_elapsed_hrs'] = (
                df.at[i, 'one_hour_min_elapsed_hrs'] + current_row['gap_hours_sub'] + next_row['total_hours']
            )

        # Calculate one_hour_top_up based on min_elapsed_hrs condition
        if df.at[i + 1, 'one_hour_min_elapsed_hrs'] >= 1:
            df.at[i + 1, 'one_hour_top_up'] = 0
        elif df.at[i + 1, 'one_hour_min_elapsed_hrs'] + current_row['gap_hours_sub'] <= 1:
            df.at[i + 1, 'one_hour_top_up'] = current_row['gap_hours_sub']
        else:
            df.at[i + 1, 'one_hour_top_up'] = 1 - df.at[i + 1, 'one_hour_min_elapsed_hrs']

    return df



# Apply the function to timesheet_min_top_up_cals
timesheet_min_top_up_cals = calculate_one_hour_top_up(timesheet_min_top_up_cals)

#Changes part of DQ14

timesheet_min_top_up_cals['one_hour_top_up'] = np.where(
    timesheet_min_top_up_cals['ex_1_3hrs_day'] == True,  # Condition
    0,  # Value if condition is True
    timesheet_min_top_up_cals['one_hour_top_up']  # Value if condition is False
)

# Save a sample to Excel for verification
one_hour_sample_output_file = os.path.join(output_tests, 'one_hour_top_up_sample_corrected.xlsx')
timesheet_min_top_up_cals[['EMPLID', 'datetime_startwork', 'total_hours', 'gap_hours_sub', 'one_hour_min_elapsed_hrs',
                           'one_hour_top_up']].head(200).to_excel(one_hour_sample_output_file, index=False)
print(f"One-Hour Top-Up sample saved to: {one_hour_sample_output_file}")

# Step 10: Adjust base rate for OT and date conditions
def adjust_base_rate(row):
    if row['PIN_NM'] == 'OT' and row['DATE WORKED'] < pd.Timestamp('2017-05-13'):
        return row['base_rate'] / 1.25
    else:
        return row['base_rate']

# Apply the base rate adjustment to a new column 'adjusted_base_rate'
timesheet_min_top_up_cals['adjusted_base_rate'] = timesheet_min_top_up_cals.apply(adjust_base_rate, axis=1)

# Calculate three_hour_top_up_cash where minimum_hours == 3
timesheet_min_top_up_cals['three_hour_top_up_cash'] = np.where(
    timesheet_min_top_up_cals['minimum_hours'] == 3,
    timesheet_min_top_up_cals['three_hour_top_up'] * timesheet_min_top_up_cals['adjusted_base_rate'],
    0  # Else set to 0
)

# Calculate one_hour_top_up_cash only if minimum_hours != 2
timesheet_min_top_up_cals['one_hour_top_up_cash'] = np.where(
    timesheet_min_top_up_cals['minimum_hours'] != 2,
    timesheet_min_top_up_cals['one_hour_top_up'] * timesheet_min_top_up_cals['adjusted_base_rate'],
    0  # Else set to 0
)

# Calculate two_hour_top_up_cash only if minimum_hours = 2
timesheet_min_top_up_cals['two_hour_top_up_cash'] = np.where(
    timesheet_min_top_up_cals['minimum_hours'] == 2,
    timesheet_min_top_up_cals['two_hour_top_up'] * timesheet_min_top_up_cals['adjusted_base_rate'],
    0  # Else set to 0
)

# Step 11: Output the final table to Parquet
timesheet_min_top_up_cals.to_parquet(output_cleaned_data + 'timesheet_min_top_up_cals.parquet', index=False)



# Check rows before 01/01/2023
print(timesheet_min_top_up_cals[timesheet_min_top_up_cals['DATE WORKED'] < pd.Timestamp('2023-01-01')][['DATE WORKED', 'three_hour_top_up']])

# Check rows on or after 01/01/2023
print(timesheet_min_top_up_cals[timesheet_min_top_up_cals['DATE WORKED'] >= pd.Timestamp('2023-01-01')][['DATE WORKED', 'three_hour_top_up']])

