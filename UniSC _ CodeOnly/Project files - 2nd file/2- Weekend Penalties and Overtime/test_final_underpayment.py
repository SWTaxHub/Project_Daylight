import pandas as pd
import os
import numpy as np
import datetime

# Step 1: Define paths for files and output directories
#timesheet_min_top_up_cals_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet'
#timesheet_min_top_up_cals_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet"
#Penalties_Recalc = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\timesheet_min_top_up_cals_Super.parquet"
#Penalties_Recalc = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_daily_weekly.parquet"
Penalties_Recalc = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_with_student_and_perm_indicator.parquet"







#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"
# Step 2: Load the Parquet files
#timesheet_min_top_up_cals = pd.read_parquet(timesheet_min_top_up_cals_path)
Penalties_Recalc = pd.read_parquet(Penalties_Recalc)

# Step 3: Find all unique EMPID_week_id where any of the specified overtime conditions are met in timesheet_cas_OT_daily_weekly
condition_ot = (
    (Penalties_Recalc['cal_weekly_ot_hours'] > 0) |
    (Penalties_Recalc['cal_daily_ot_hours'] > 0) |
    (Penalties_Recalc['cal_ot_span_as_hours'] > 0) |
    (Penalties_Recalc['cal_ot_span_bs_hours'] > 0) |
    (Penalties_Recalc['cal_ot_span_weekend_hours'] > 0) |
    (Penalties_Recalc['OT_Cas_Loading_Discrp'] != 0)
)

# Filter for unique EMPID_week_id where overtime conditions are met
empid_week_id_with_overtime = Penalties_Recalc.loc[condition_ot, 'EMPLID_week_id'].unique()

# Step 4: Find all unique date_only where any of the top-up cash fields are greater than 0 in timesheet_min_top_up_cals
condition_top_up = (
    (Penalties_Recalc['Meal_Allowance'] > 0) |
    (Penalties_Recalc['wknd_discrepancy_amount_excl'] > 0) 
)




# Filter for unique date_only with non-zero top-up cash values
dates_with_top_up_cash = Penalties_Recalc.loc[condition_top_up, 'EMPLID_week_id'].unique()

# Step 5: Filter transactions in timesheet_cas_OT_daily_weekly where EMPID_week_id matches those with overtime or where date_only matches those with top-up cash
matching_transactions = Penalties_Recalc[
    (Penalties_Recalc['EMPLID_week_id'].isin(empid_week_id_with_overtime)) |
    (Penalties_Recalc['EMPLID_week_id'].isin(dates_with_top_up_cash))
]

# step 5b: Restrict extract to only hours worked after 01/01/2017

# Ensure 'DATE WORKED' is in datetime format using .loc
matching_transactions.loc[:, 'DATE WORKED'] = pd.to_datetime(matching_transactions['DATE WORKED'], errors='coerce')

# Filter rows where 'DATE WORKED' is after 01/01/2017
matching_transactions = matching_transactions[matching_transactions['DATE WORKED'] >= '2017-01-01']

# Add code block to make values past 30/06/2024 null for several columns

# added 26/02/2025

# Convert DATE_WORKED to datetime format
matching_transactions['DATE WORKED'] = pd.to_datetime(matching_transactions['DATE WORKED'], dayfirst=True)

# Define the cutoff date
cutoff_date = pd.Timestamp("2024-06-30")

# List of columns to overwrite with NaN
# columns_to_nullify = [
#     "cal_ot_span_weekend_hours", "cal_ot_span_as_hours", "cal_ot_span_bs_hours",
#     "penalty_reptm", "date_only", "adjusted_daily_hours", "daily_hrs_count",
#     "cal_daily_ot_hours", "incremental_daily_ot_hours", "week_start_date",
#     "EMPLID_week_id", "adjusted_weekly_hours", "weekly_hrs_count", 
#     "cal_weekly_ot_hours", "incremental_weekly_ot_hours", "cal_OT_hours", 
#     "cal_sunday_ot", "timesheet", "cal_PH_ot", "cummulative_cal_OT_hours",
#     "prior_cummulative_cal_OT_hours", "cal_first_3_ot", "cal_post_3_ot",
#     "cal_wknd_penalty_sat", "cal_wknd_penalty_sun", "cal_balance_hours", 
#     "ts_factor", "cal_factor_incl", "cal_factor_excl", "cal_factor_may17",
#     "factor_difference_incl", "discrepancy_amount_incl", "factor_difference_excl",
#     "discrepancy_amount_excl", "factor_difference_may17", "discrepancy_amount_may17",
#     "cal_shift_top_up", "Cal_OT_Hours_Aggregated", "G_Break_Minutes_Aggregated",
#     "ENDDTTM_timeOnly", "Meal_Allowance_Code", "Meal_Allowance", "is_student",
#     "is_perm", "minimum_hours", "gap_hours", "conseq_cumul_sumhrs", "EMPLID_date_only",
#     "ex_1_3hrs_day", "gap_hours_sub", "is_first_shift", "cond_new_emplid",
#     "cond_new_date", "cond_midnight_straddle", "cond_midnight_carryover",
#     "first_prior_gap_found", "elapsed_hrs", "three_hour_top_up", "one_hour_top_up",
#     "two_hour_top_up", "condition_1_position_nbr", "condition_2_elapsed_hrs",
#     "condition_3_ex_1_3hrs_day", "condition_4_date_worked", "condition_5_gap_and_elapsed",
#     "one_hour_min_elapsed_hrs", "adjusted_base_rate", "three_hour_top_up_cash",
#     "one_hour_top_up_cash", "two_hour_top_up_cash", "sum_WeekendPens",
#     "Weekend_Pens_Factor", "Weekend_Pens_DollarCalcAmt", "weekendPensComp",
#     "recalc_Weekend_Pens", "Total_Shortfall_excl_Interest", "Wages Paid Date",
#     "compInterestFactor", "recalc_Weekend_Pens_wthInterest", "3hrTopup_withInterest",
#     "2hrTopup_withInterest", "1hrTopup_withInterest", "cal_shift_topup_withInterest",
#     "Super_from_weekendPens", "Super_from_3hrTopup", "Super_from_2hrTopup",
#     "Super_from_1hrTopup", "Super_from_CasualShiftTopup", "Total_Super_Shortfall"
# ]

columns_to_nullify = [
    "cal_ot_span_weekend_hours", "cal_ot_span_as_hours", "cal_ot_span_bs_hours",
    "penalty_reptm", "date_only", "adjusted_daily_hours", "daily_hrs_count",
    "cal_daily_ot_hours", "incremental_daily_ot_hours", "week_start_date",
    "EMPLID_week_id", "adjusted_weekly_hours", "weekly_hrs_count", 
    "cal_weekly_ot_hours", "incremental_weekly_ot_hours", "cal_OT_hours", 
    "cal_sunday_ot", "timesheet", "cal_PH_ot", "cummulative_cal_OT_hours",
    "prior_cummulative_cal_OT_hours", "cal_first_3_ot", "cal_post_3_ot",
    "cal_wknd_penalty_sat", "cal_wknd_penalty_sun", "cal_balance_hours", 
    "ts_factor", "cal_factor_incl", "cal_factor_excl", "cal_factor_may17",
    #"factor_difference_incl", "discrepancy_amount_incl",
     "factor_difference_excl",
    "discrepancy_amount_excl", # "factor_difference_may17", "discrepancy_amount_may17",
    "cal_shift_top_up", "Cal_OT_Hours_Aggregated", "G_Break_Minutes_Aggregated",
    "ENDDTTM_timeOnly", "Meal_Allowance_Code", "Meal_Allowance", #"is_student",
    #"is_perm", "minimum_hours", "gap_hours", "conseq_cumul_sumhrs", "EMPLID_date_only",
    #"ex_1_3hrs_day", "gap_hours_sub", "is_first_shift", "cond_new_emplid",
    #"cond_new_date", "cond_midnight_straddle", "cond_midnight_carryover",
    #"first_prior_gap_found", "elapsed_hrs", "three_hour_top_up", "one_hour_top_up",
    #"two_hour_top_up", "condition_1_position_nbr", "condition_2_elapsed_hrs",
    #"condition_3_ex_1_3hrs_day", "condition_4_date_worked", "condition_5_gap_and_elapsed",
    #"one_hour_min_elapsed_hrs", "adjusted_base_rate", "three_hour_top_up_cash",
    #"one_hour_top_up_cash", "two_hour_top_up_cash", "sum_WeekendPens",
    "avg_cal_loading", "average_ts_loading", "wknd_discrepancy_amount_excl"
    #"Weekend_Pens_Factor", "Weekend_Pens_DollarCalcAmt", "weekendPensComp",
    #"recalc_Weekend_Pens", "Total_Shortfall_excl_Interest", "Wages Paid Date",
    #"compInterestFactor", "recalc_Weekend_Pens_wthInterest", "3hrTopup_withInterest",
    #"2hrTopup_withInterest", "1hrTopup_withInterest", "cal_shift_topup_withInterest",
    #"Super_from_weekendPens", "Super_from_3hrTopup", "Super_from_2hrTopup",
    #"Super_from_1hrTopup", "Super_from_CasualShiftTopup", "Total_Super_Shortfall"
]





# Apply condition to set selected columns to NaN
matching_transactions.loc[matching_transactions['DATE WORKED'] > cutoff_date, columns_to_nullify] = np.nan













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
# matching_transactions['Is_Duplicate'] = matching_transactions.groupby(['DATE WORKED', 'EMPLID'])['EMPLID'].transform('size') > 1

# # Filter the DataFrame to keep only rows where the condition is true
# condensed_data = matching_transactions[matching_transactions['Is_Duplicate']]



# # Display the condensed DataFrame
# print(condensed_data)
# condensed_data.to_csv('potential_rule_breakers.csv')

print("Test to see what columns remain after calculations")
print(list(matching_transactions.columns))

# Step 6: Output the matching transactions to Excel as a sample
current_date = datetime.datetime.now().strftime('%Y-%m-%d')
output_file = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_WkdPens_MAlw_CasLoad{current_date}.xlsx')
matching_transactions.head(100000).to_excel(output_file, index=False)

print(f"Sample of matching transactions with overtime and top-up cash saved to {output_file}")
# Print the number of rows in the matching_transactions DataFrame
print(f"Number of rows in matching_transactions: {len(matching_transactions)}")

