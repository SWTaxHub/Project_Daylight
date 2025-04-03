import pandas as pd
import os
import numpy as np
import datetime


cleaned_data = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"
output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"
current_date = datetime.datetime.now().strftime('%Y-%m-%d')


#file1 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_OT_ME{current_date}.xlsx')
file1 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_OT_ME2025-04-01.xlsx')

#file2 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_WkdPens_MAlw_CasLoad{current_date}.xlsx')
file2 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_WkdPens_MAlw_CasLoad2025-04-01.xlsx')

man_invest = os.path.join(cleaned_data, f'Manual_Investigation_INDEX_CODES.xlsx')




OT_ME_topups = pd.read_excel(file1)
                             #, index_col=0)
WkdPens_MAlw_CasLoad = pd.read_excel(file2)
                                     #, index_col=0)

man_invest_indexes = pd.read_excel(man_invest)

print("man_invest_indexes duplicates: ")

print(man_invest_indexes['index'].duplicated().sum())  # Should print 0

merged_df = man_invest_indexes.drop_duplicates(subset=['index'], keep='last')


# Define the columns you want to keep
columns_to_keep1 = ['index', 'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID', 
                    'Department Name', 'GL_Cost_Account', 'GP_RATE', 'Grade-Step OR Course Code', 'POSITION_NBR', 'Position Title',
                    'REPORTS_TO', 'manual_excl', 'is_student', 'is_perm',
                    'discrepancy_amount_excl', 'cal_shift_top_up', 
                    'three_hour_top_up_cash', 
                   'one_hour_top_up_cash', 'two_hour_top_up_cash']  

# Filter the DataFrame to keep only these columns
OT_ME_topups = OT_ME_topups[columns_to_keep1].fillna(0)






columns_to_keep2 = ['index', 'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID', 
                    'Department Name', 'GL_Cost_Account', 'GP_RATE', 'Grade-Step OR Course Code', 'POSITION_NBR', 'Position Title',
                    'REPORTS_TO', 'manual_excl', 
                    'is_student', 'is_perm',
                    'wknd_discrepancy_amount_excl', 'OT_Cas_Loading_Discrp', 'Meal_Allowance']

WkdPens_MAlw_CasLoad = WkdPens_MAlw_CasLoad[columns_to_keep2].fillna(0)


WkdPens_MAlw_CasLoad.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_TestOTCASLOAD{current_date}.csv'), index=False)



OT_ME_topups.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_TestOTME{current_date}.csv'), index=False)


# Merge the dataframes
# merged = pd.merge(OT_ME_topups, WkdPens_MAlw_CasLoad, on=['eFORM_ID', 'NAME', 'EMPLID', 'DATE WORKED', 'BEGINDTTM', 'ENDDTTM'], how='outer')



merged = pd.merge(OT_ME_topups, WkdPens_MAlw_CasLoad, on=['index', 'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID', 
                    'Department Name', 'GL_Cost_Account', 'GP_RATE', 'Grade-Step OR Course Code', 
                    #'POSITION_NBR', 'Position Title',
                    'REPORTS_TO', 'manual_excl'], how='outer')


merged['Position Title_x'] = merged['Position Title_x'].fillna(merged['Position Title_y'])
merged['Position Title_x'] = merged['Position Title_x'].rename('Position Title')
merged.drop(['Position Title_y'], axis=1, inplace=True)

merged["POSITION_NBR_x"] = merged["POSITION_NBR_x"].fillna(merged["POSITION_NBR_y"])
merged["POSITION_NBR_x"] = merged["POSITION_NBR_x"].rename("POSITION_NBR") 
merged.drop(["POSITION_NBR_y"], axis=1, inplace=True)



merged['is_student_x'] = merged['is_student_x'].fillna(merged['is_student_y'])
merged['is_perm_x'] = merged['is_perm_x'].fillna(merged['is_perm_y'])  
merged['is_perm_x'] = merged['is_perm_x'].astype(bool)
merged['is_student_x'] = merged['is_student_x'].astype(bool)
merged.drop(['is_student_y', 'is_perm_y'], axis=1, inplace=True)


#merged['index_Combined'] = merged['index_x'].fillna(merged['index_y'])




print('merged columns before update: ')
print(merged.columns)

# Define the columns you want to update if they are NaN or 0
cols_to_update = [
    #'POSITION_NBR', 'Position Title', 
    'discrepancy_amount_excl', 'cal_shift_top_up', 'three_hour_top_up_cash',
    'one_hour_top_up_cash', 'two_hour_top_up_cash', 'wknd_discrepancy_amount_excl',
    'OT_Cas_Loading_Discrp', 'Meal_Allowance'
]



# Replace 0 with NaN to allow proper filling
#merged[cols_to_update] = merged[cols_to_update].replace(0, np.nan)

# Fill NaN values with the max available for each 'index'
merged[cols_to_update] = merged.groupby('index')[cols_to_update].transform('max')

merged.columns = merged.columns.str.strip()  # Removes spaces before/after column names


print('merged columns after update: ')
print(merged.columns)

cols_to_fill = ['POSITION_NBR', 'Position Title']

# Only apply transformation if columns exist
existing_cols_to_fill = [col for col in cols_to_fill if col in merged.columns]

if existing_cols_to_fill:
    #merged[existing_cols_to_fill] = merged.groupby('index')[existing_cols_to_fill].transform(lambda x: x.ffill().bfill())
    merged[existing_cols_to_fill] = merged.groupby('index')[existing_cols_to_fill].transform('max')
else:
    print("Some columns are missing:", [col for col in cols_to_fill if col not in merged.columns])


print('merged columns after filling: ')
print(merged.columns)





print(merged['index'].dtype)
print(man_invest_indexes['index'].dtype)



import pandas as pd

# Perform a merge on 'index', and bring in only the 'Work Area MI Outcome' column from man_invest_indexes
merged_df = merged.merge(
    man_invest_indexes[['index', 'Work Area MI Outcome', 'Work Area Evidence']], 
    on='index', 
    how='left',  # Left join to keep all rows from merged
    suffixes=('', '_man_invest')
)

# Define the columns to keep
columns_to_keep = [
    'index', 'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED',
    'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID',
    'Department Name', 'GL_Cost_Account', 'GP_RATE',
    'Grade-Step OR Course Code', 'POSITION_NBR_x', 'Position Title_x',
    'REPORTS_TO', 'manual_excl', 'is_student_x', 'is_perm_x',
    'discrepancy_amount_excl', 'cal_shift_top_up', 'three_hour_top_up_cash',
    'one_hour_top_up_cash', 'two_hour_top_up_cash',
    'wknd_discrepancy_amount_excl', 'OT_Cas_Loading_Discrp',
    'Meal_Allowance', 'Work Area MI Outcome', 'Work Area Evidence' # Ensure this is included
]

# Keep only the required columns
merged_df = merged_df[columns_to_keep]

# Rename columns for consistency
merged_df.rename(columns={
    'POSITION_NBR_x': 'POSITION_NBR',
    'Position Title_x': 'Position Title',
    'is_student_x': 'is_student',
    'is_perm_x': 'is_perm'
}, inplace=True)

# Drop duplicated rows based on 'index', keeping the last occurrence
merged_df = merged_df.drop_duplicates(subset=['index'], keep='last')

# Print final structure for validation
print(merged_df.columns)
print(merged_df.head())


print(merged_df['index'].duplicated().sum())  # Should print 0

merged_df = merged_df.drop_duplicates(subset=['index'], keep='last')


print('merged_df Worlk Area MI Outcome: ')

print(merged_df['Work Area MI Outcome'].unique())


print(merged_df.columns)


import numpy as np

merged_df['discrepancy_amount_excl'] = np.where(
    merged_df['Work Area MI Outcome'] == 'OT NOT PAYABLE',  
    0,  # If condition is met, set to 0
    merged_df['discrepancy_amount_excl']  # Otherwise, keep existing value
)

merged_df['one_hour_top_up_cash'] = np.where(
    merged_df['Work Area MI Outcome'] == '3 hour',
    0,
     merged_df['one_hour_top_up_cash']
)

merged_df['three_hour_top_up_cash'] = np.where(
    (merged_df['Work Area MI Outcome'] == '1 hour'),  # Condition on 'Work Area MI Outcome'
    0,  # Set to 0 if condition is true
    merged_df['three_hour_top_up_cash']  # Else, keep the original value
)
# Apply np.where
merged_df['OT_Cas_Loading_Discrp'] = np.where(
    (merged_df['Work Area MI Outcome'] == 'OT NOT PAYABLE') & 
    (merged_df['Work Area Evidence'] == 'SMI 11 Rolfe Misclaim'),
    0,  
    merged_df['OT_Cas_Loading_Discrp']
)

#merged_df['wknd_discrepancy_amount_excl']





merged_df['Meal_Allowance'] = np.where(
    (merged_df['Work Area MI Outcome'] == 'OT NOT PAYABLE') & 
    (merged_df['Work Area Evidence'] == 'SMI 11 Rolfe Misclaim') |
    (merged_df['Work Area Evidence'] == 'DDC Review 2022') ,
    0,  
    merged_df['Meal_Allowance']
)

merged_df.to_csv(os.path.join(output_tests, f'mergedDFTest{current_date}.csv'), index=False)


merged_df.rename(columns={
    #'index_x': 'OT_ME_topups_Index',
    #'index_y': 'WkdPens_MAlw_CasLoad_Index',
    'Position Title_x': 'Position Title',
    'POSITION_NBR_x': 'POSITION_NBR',
    'is_perm_x': 'is_perm',
    'is_student_x': 'is_student'
}, inplace=True)







# reorder columns
merged_df = merged_df[[#'index_Combined', 'OT_ME_topups_Index', 'WkdPens_MAlw_CasLoad_Index',
                'index',  'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 
                 'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID','Department Name', 'GL_Cost_Account', 'GP_RATE', 'Grade-Step OR Course Code',
                   'POSITION_NBR', 'Position Title', 'REPORTS_TO', 'manual_excl', 'is_student', 'is_perm', 'discrepancy_amount_excl', 'cal_shift_top_up', 
                   'three_hour_top_up_cash', 'one_hour_top_up_cash', 'two_hour_top_up_cash','wknd_discrepancy_amount_excl', 'OT_Cas_Loading_Discrp', 'Meal_Allowance']]


merged_df = merged_df[~merged_df.duplicated(subset=['index'], keep='last')]


#Commented out on 19/12/24 Interest Calcs - USC have asked for the interest free outputs






interest_rates = pd.read_csv(r"c:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\InterestRates.csv")

print(interest_rates.columns)

interest_rates['Wages Paid Date'].rename('DATE WORKED', inplace=True)


print("Missing 'DATE WORKED' in merged_df:", merged_df['DATE WORKED'].isna().sum())
print("Missing 'DATE WORKED' in interest_rates:", interest_rates['DATE WORKED'].isna().sum())


with_interest_rate = merged_df.merge(interest_rates, on='DATE WORKED', how='left')

#Initialise recalc weekend Penalities with Interest Applied column 

print(with_interest_rate.columns)
print(with_interest_rate['compInterestFactor'].unique())
print(with_interest_rate['compInterestFactor'].dtype)
print(with_interest_rate['compInterestFactor'].head())




# merged_df['Weekend_Pens_wthInterest'] = 0

# #recalc weekend pens with Interest applied 
# merged_df['rWeekend_Pens_wthInterest'] = merged_df['recalc_Weekend_Pens'] * interest_rates['compInterestFactor'] 

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










# # Drop duplicate rows
# #merged.drop_duplicates(inplace=True)

# # Display the first few rows
# print(merged_df.head())


# merged_df.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_merged{current_date}.csv'), index=False)
# # List of columns to sum
# # columns_to_sum = [
# #     'discrepancy_amount_excl', 'cal_shift_top_up', 'one_hour_top_up_cash',
# #     'two_hour_top_up_cash', 'three_hour_top_up_cash', 'wknd_discrepancy_amount_excl',
# #     'OT_Cas_Loading_Discrp', 'Meal_Allowance'
# # ]

