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


conditions = [
    {
        "condition": (
            (timesheet_df['cal_wknd_penalty_sat'] > 0) &  
            (timesheet_df['cal_wknd_penalty_sun'] == 0) 
        ),
        "true_value": 0.2,
        "false_value": 0,
    },
    {
        "condition": (
            (timesheet_df['cal_wknd_penalty_sat'] == 0) &  
            (timesheet_df['cal_wknd_penalty_sun'] > 0) 
        ),
        "true_value": 0.5,
        "false_value": 0,
    },
   
]






# Initialise the Weekened Penality Factor column with default values
timesheet_df['Weekend_Pens_Factor'] = 0


# Apply conditions iteratively for Wkend Pen Factor
for cond in conditions:
    timesheet_df['Weekend_Pens_Factor'] = np.where(
        cond["condition"],
        cond["true_value"],  # Value if condition is True
        timesheet_df['Weekend_Pens_Factor']  # Keep the current value if False

    )

# Initialise the Weekend Pen Dollar Amount column with default values
timesheet_df['Weekend_Pens_DollarCalcAmt'] = 0 


# Need to multipe
# Multiple sum_WeekendPens by Base Rate and apply Sat or Sun loading to give the dollar amount owed 
timesheet_df['Weekend_Pens_DollarCalcAmt'] = timesheet_df['sum_WeekendPens'] * timesheet_df['base_rate'] * timesheet_df['Weekend_Pens_Factor']



#Compare this dollar amount with the discrepancyamount_excel (column DL )

timesheet_df['weekendPensComp'] = timesheet_df['Weekend_Pens_DollarCalcAmt'] - timesheet_df['discrepancy_amount_excl']


# Initialise the recalc Weekend Pens column with default values
timesheet_df['recalc_Weekend_Pens'] = 0 

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

# Apply conditions iteratively for recalc Weekend Pens
for cond in conditions_recalc:
    timesheet_df['recalc_Weekend_Pens'] = np.where(
        cond["condition"],
        cond["true_value"],  # Value if condition is True
        cond["false_value"]  # Keep the current value if False
    )



#Super Guarentee Rate
sgRate = 0.115

#Super Payments for 3 hour top up 


timesheet_df['Super_from_3hrTopup'] = timesheet_df['three_hour_top_up_cash'] * sgRate


#Super Payments for 2 hour top up
timesheet_df['Super_from_2hrTopup'] = timesheet_df['two_hour_top_up_cash'] * sgRate

# #Super Payments for 1 hour top up

timesheet_df['Super_from_1hrTopup'] = timesheet_df['one_hour_top_up_cash'] * sgRate


# #Casual shift top up
timesheet_df['Super_from_CasualShiftTopup'] = timesheet_df['cal_shift_top_up'] * sgRate

#Initialise Interest Compound Interest Factor column

timesheet_df['compInterestFactor'] = 0 




timesheet_df.to_parquet(output_cleaned_data + 'timesheet_min_top_up_cals_Super.parquet', index=False)
timesheet_df.to_csv(output_cleaned_data + 'timesheet_min_top_up_cals_Super.csv', index=False)

'''
Formula logic 

timesheet_df['compInterestFactor'] Where date_only equals date from compound interest factor table 
then get factor from the compound interest factor table for that row 



'''



#Commented out on 19/12/24 Interest Calcs - USC have asked for the interest free outputs



# final_timesheet_df = pd.read_csv(r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\weekendPenaltiesRecalcswihInterest.csv")

# #Initialise recalc weekend Penalities with Interest Applied column 






# final_timesheet_df['recalc_Weekend_Pens_wthInterest'] = 0

# #recalc weekend pens with Interest applied 
# final_timesheet_df['recalc_Weekend_Pens_wthInterest'] = final_timesheet_df['recalc_Weekend_Pens'] * final_timesheet_df['compInterestFactor'] 

# #3 hr top with Interest applied 
# final_timesheet_df['3hrTopup_withInterest'] = final_timesheet_df['three_hour_top_up_cash'] * final_timesheet_df['compInterestFactor'] 

# #2hr top up with Interest Applied
# final_timesheet_df['2hrTopup_withInterest'] = final_timesheet_df['two_hour_top_up_cash'] * final_timesheet_df['compInterestFactor'] 

# #1hr top up with Interest applied 
# final_timesheet_df['1hrTopup_withInterest'] = final_timesheet_df['one_hour_top_up_cash'] * final_timesheet_df['compInterestFactor'] 

# #Calc Shift with Interest Applied
# final_timesheet_df['cal_shift_topup_withInterest'] = final_timesheet_df['cal_shift_top_up'] * final_timesheet_df['compInterestFactor']



# #Super Payments for Weekend Pens
# final_timesheet_df['Super_from_weekendPens'] = final_timesheet_df['recalc_Weekend_Pens_wthInterest'] * sgRate


# #Super Payments for 3 hour top up 
# final_timesheet_df['Super_from_3hrTopup'] = final_timesheet_df['3hrTopup_withInterest'] * sgRate


# #Super Payments for 2 hour top up
# final_timesheet_df['Super_from_2hrTopup'] = final_timesheet_df['2hrTopup_withInterest'] * sgRate

# #Super Payments for 1 hour top up
# final_timesheet_df['Super_from_1hrTopup'] = final_timesheet_df['one_hour_top_up_cash'] * sgRate


# #Casual shift top up
# final_timesheet_df['Super_from_CasualShiftTopup'] = final_timesheet_df['cal_shift_topup_withInterest'] * sgRate







# final_timesheet_df.to_csv('final_output.csv')

# #print(timesheet_df.head)

# #timesheet_df.to_csv('weekendPenaltiesRecalcs.csv')




# #Additional columns to calculate super on 

# #three_hour_top_up_cash
# #one_hour_top_up_cash
# #two_hour_top_up_cash
# #cal_shift_top_up

