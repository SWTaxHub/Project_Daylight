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
#timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals_MealAllowance.parquet"

#timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet"

#timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals_MealAllowance.parquet"


timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet"



output_cleaned_data = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"

output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"


# Load the timesheet data
timesheet_df = pd.read_parquet(timesheet_path)

#There are never any values in both Sat and Sun columns so add the values together
timesheet_df['sum_WeekendPens'] = (
    ((timesheet_df['cal_wknd_penalty_sat']) + (timesheet_df['cal_wknd_penalty_sun']))
    -
    ((timesheet_df['SATCASUAL_hours']) + (timesheet_df['SUNCASUAL_hours']))
    
).clip(lower=0)
# For the above limit values to zero no negatives values

timesheet_df['cal_wknd_penalty_sat'] = pd.to_numeric(timesheet_df['cal_wknd_penalty_sat']) 
timesheet_df['cal_wknd_penalty_sun'] = pd.to_numeric(timesheet_df['cal_wknd_penalty_sun'])


# conditions = [
#     {
#         "condition": (
#             (timesheet_df['cal_wknd_penalty_sat'] > 0) &  
#             (timesheet_df['cal_wknd_penalty_sun'] == 0) 
#         ),
#         "true_value": 0.2,
#         "false_value": 0,
#     },
#     {
#         "condition": (
#             (timesheet_df['cal_wknd_penalty_sat'] == 0) &  
#             (timesheet_df['cal_wknd_penalty_sun'] > 0) 
#         ),
#         "true_value": 0.5,
#         "false_value": 0,
#     },
   
# ]



# # Initialise the Weekened Penality Factor column with default values
# timesheet_df['Weekend_Pens_Factor'] = 0

# timesheet_df['Weekend_Pens_Factor'] = pd.to_numeric(timesheet_df['Weekend_Pens_Factor'])

# # Apply conditions iteratively for Wkend Pen Factor
# for cond in conditions:
#     timesheet_df['Weekend_Pens_Factor'] = np.where(
#         cond["condition"],
#         cond["true_value"],  # Value if condition is True
#         timesheet_df['Weekend_Pens_Factor']  # Keep the current value if False

#     )




# Example: your DataFrame (ensure you replace this with your actual timesheet_df)
# timesheet_df = pd.DataFrame(...)

# Define the conditions
conditions = [
    {
        "condition": (
            (timesheet_df['cal_wknd_penalty_sat'] > 0) &  
            (timesheet_df['cal_wknd_penalty_sun'] == 0) 
        ),
        "true_value": 0.2,
        "false_value": 0,  # Not used, since we directly update with .loc
    },
    {
        "condition": (
            (timesheet_df['cal_wknd_penalty_sat'] == 0) &  
            (timesheet_df['cal_wknd_penalty_sun'] > 0) 
        ),
        "true_value": 0.5,
        "false_value": 0,  # Not used
    },
    # Add more conditions if needed
]

# Initialise the Weekend Penalty Factor column with default values (0)
timesheet_df['Weekend_Pens_Factor'] = 0

# Apply conditions iteratively using .loc
for cond in conditions:
    timesheet_df.loc[cond["condition"], 'Weekend_Pens_Factor'] = cond["true_value"]

# At this point, 'Weekend_Pens_Factor' is updated with the values based on your conditions.


# Initialise the Weekend Pen Dollar Amount column with default values
timesheet_df['Weekend_Pens_DollarCalcAmt'] = 0 
timesheet_df['Weekend_Pens_DollarCalcAmt'] = pd.to_numeric(timesheet_df['Weekend_Pens_DollarCalcAmt'])
timesheet_df['sum_WeekendPens'] = pd.to_numeric(timesheet_df['sum_WeekendPens'])
timesheet_df['base_rate'] = pd.to_numeric(timesheet_df['base_rate'])

# Need to multipe
# Multiple sum_WeekendPens by Base Rate and apply Sat or Sun loading to give the dollar amount owed 
timesheet_df['Weekend_Pens_DollarCalcAmt'] = timesheet_df['sum_WeekendPens'] * timesheet_df['base_rate'] * timesheet_df['Weekend_Pens_Factor']



#Compare this dollar amount with the discrepancyamount_excel (column DL )

timesheet_df['discrepancy_amount_excl'] = pd.to_numeric(timesheet_df['discrepancy_amount_excl'])

timesheet_df['weekendPensComp'] = 0
timesheet_df['weekendPensComp'] = pd.to_numeric(timesheet_df['weekendPensComp'])


timesheet_df['weekendPensComp'] = timesheet_df['Weekend_Pens_DollarCalcAmt'] - timesheet_df['discrepancy_amount_excl']



# Initialise the recalc Weekend Pens column with default values
timesheet_df['recalc_Weekend_Pens'] = 0 
timesheet_df['recalc_Weekend_Pens'] = pd.to_numeric(timesheet_df['recalc_Weekend_Pens'])

conditions_recalc = [
    {
        "condition": (timesheet_df['weekendPensComp'] < 0),
        "true_value": timesheet_df['discrepancy_amount_excl'],
        "false_value": timesheet_df['recalc_Weekend_Pens']
    },
    {
        "condition": (timesheet_df['weekendPensComp'] > 0),
        "true_value": timesheet_df['Weekend_Pens_DollarCalcAmt'],
        "false_value": timesheet_df['recalc_Weekend_Pens']
    }
]


# Apply conditions iteratively using .loc
for cond in conditions_recalc:
    timesheet_df.loc[cond["condition"], 'recalc_Weekend_Pens'] = cond["true_value"]


timesheet_df['Total_Shortfall_excl_Interest'] = timesheet_df['one_hour_top_up_cash'] + \
                                                timesheet_df['two_hour_top_up_cash'] + \
                                                timesheet_df['three_hour_top_up_cash'] + \
                                                timesheet_df['recalc_Weekend_Pens'] + \
                                                timesheet_df['cal_shift_top_up']



#Initialise Interest Compound Interest Factor column

#timesheet_df['compInterestFactor'] = 0 






'''
Formula logic 

timesheet_df['compInterestFactor'] Where date_only equals date from compound interest factor table 
then get factor from the compound interest factor table for that row 



'''


#Super Guarentee Rate
sgRate = 0.115


interestRates = pd.read_csv(r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\InterestRates.csv")



# Ensure both columns are in datetime format
timesheet_df['Pay Date'] = pd.to_datetime(timesheet_df['Pay Date'], errors='coerce')
interestRates['Wages Paid Date'] = pd.to_datetime(interestRates['Wages Paid Date'], errors='coerce')





# Get unique values
timesheet_dates = set(timesheet_df['Pay Date'].unique())
interest_rates_dates = set(interestRates['Wages Paid Date'].unique())

# Find matches and non-matches
matching_dates = timesheet_dates.intersection(interest_rates_dates)
non_matching_timesheet = timesheet_dates - interest_rates_dates
non_matching_interest_rates = interest_rates_dates - timesheet_dates

# Print results
print(f"Number of matching dates: {len(matching_dates)}")
print(f"Number of non-matching dates in timesheet_df: {len(non_matching_timesheet)}")
print(f"Number of non-matching dates in interestRates: {len(non_matching_interest_rates)}")



# Find non-matching dates
non_matching_dates = list(timesheet_dates.symmetric_difference(interest_rates_dates))
print(f"Number of non-matching dates: {len(non_matching_dates)}")
print("List of non-matching dates:", non_matching_dates)



# Now merge
timesheet_df = timesheet_df.merge(
    interestRates[['Wages Paid Date', 'Daily Simple Factor']], 
    left_on='Pay Date', 
    right_on='Wages Paid Date', 
    how='left'
)

# Fill NaN values if no match was found
timesheet_df['Daily Simple Factor'] = timesheet_df['Daily Simple Factor'].fillna(0)

# Rename for clarity
timesheet_df.rename(columns={'Daily Simple Factor': 'compInterestFactor'}, inplace=True)



#Commented out on 19/12/24 Interest Calcs - USC have asked for the interest free outputs



# final_timesheet_df = pd.read_csv(r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\weekendPenaltiesRecalcswihInterest.csv")

# #Initialise recalc weekend Penalities with Interest Applied column 




timesheet_df['recalc_Weekend_Pens_wthInterest'] = np.where(
    timesheet_df['recalc_Weekend_Pens'].notna() & (timesheet_df['recalc_Weekend_Pens'] != 0), 
    timesheet_df['recalc_Weekend_Pens'] * 1 + timesheet_df['compInterestFactor'], 
    0
)

timesheet_df['3hrTopup_withInterest'] = np.where(
    timesheet_df['three_hour_top_up_cash'].notna() & (timesheet_df['three_hour_top_up_cash'] != 0), 
    timesheet_df['three_hour_top_up_cash'] * 1 + timesheet_df['compInterestFactor'], 
    0
)

timesheet_df['2hrTopup_withInterest'] = np.where(
    timesheet_df['two_hour_top_up_cash'].notna() & (timesheet_df['two_hour_top_up_cash'] != 0), 
    timesheet_df['two_hour_top_up_cash'] * 1 + timesheet_df['compInterestFactor'], 
    0
)

timesheet_df['1hrTopup_withInterest'] = np.where(
    timesheet_df['one_hour_top_up_cash'].notna() & (timesheet_df['one_hour_top_up_cash'] != 0), 
    timesheet_df['one_hour_top_up_cash'] * 1 + timesheet_df['compInterestFactor'], 
    0
)

timesheet_df['cal_shift_topup_withInterest'] = np.where(
    timesheet_df['cal_shift_top_up'].notna() & (timesheet_df['cal_shift_top_up'] != 0), 
    timesheet_df['cal_shift_top_up'] * 1 + timesheet_df['compInterestFactor'], 
    0
)


# - takes the sum of the amounts due from the one, two and three top ups, along with the recalculated weekend penalties and casual shift top up

timesheet_df['Total_Shortfall_incl_Interest'] = timesheet_df['recalc_Weekend_Pens_wthInterest'] + \
                                                timesheet_df['1hrTopup_withInterest'] + \
                                                timesheet_df['2hrTopup_withInterest'] + \
                                                timesheet_df['3hrTopup_withInterest'] + \
                                                timesheet_df['cal_shift_topup_withInterest']


# #Super Payments for Weekend Pens
timesheet_df['Super_from_weekendPens'] = timesheet_df['recalc_Weekend_Pens_wthInterest'] * sgRate


# #Super Payments for 3 hour top up 
timesheet_df['Super_from_3hrTopup'] = timesheet_df['3hrTopup_withInterest'] * sgRate


# #Super Payments for 2 hour top up
timesheet_df['Super_from_2hrTopup'] = timesheet_df['2hrTopup_withInterest'] * sgRate

# #Super Payments for 1 hour top up
timesheet_df['Super_from_1hrTopup'] = timesheet_df['one_hour_top_up_cash'] * sgRate


# #Casual shift top up
timesheet_df['Super_from_CasualShiftTopup'] = timesheet_df['cal_shift_topup_withInterest'] * sgRate


timesheet_df['Total_Super_Shortfall'] = timesheet_df['Super_from_weekendPens'] + \
                                        timesheet_df['Super_from_1hrTopup'] + \
                                        timesheet_df['Super_from_2hrTopup'] + \
                                        timesheet_df['Super_from_3hrTopup'] + \
                                        timesheet_df['Super_from_CasualShiftTopup']




# timesheet_df.to_csv('final_output.csv')

# #print(timesheet_df.head)

# #timesheet_df.to_csv('weekendPenaltiesRecalcs.csv')




# #Additional columns to calculate super on 

# #three_hour_top_up_cash
# #one_hour_top_up_cash
# #two_hour_top_up_cash
# #cal_shift_top_up



timesheet_df.to_parquet(output_cleaned_data + 'timesheet_min_top_up_cals_Super.parquet', index=False)