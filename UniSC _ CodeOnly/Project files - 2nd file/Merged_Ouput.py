import pandas as pd
import os
import numpy as np
import datetime


cleaned_data = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"
output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"
current_date = datetime.datetime.now().strftime('%Y-%m-%d')


#file1 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_OT_ME{current_date}.xlsx')
file1 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_OT_ME2025-05-23.parquet')




#file2 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_WkdPens_MAlw_CasLoad{current_date}.xlsx')
file2 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_WkdPens_MAlw_CasLoad2025-05-23.parquet')

man_invest_path = os.path.join(cleaned_data, f'Manual_Investigation_INDEX_CODES.xlsx')



MI_emplids = [
# '9004800',
# '1090664',
# '1015162',
# '9007645',
# '1107530',
# '1098901',
# '1070218',
# '1118913'
9004800,
1090664,
1015162,
9007645,
1107530,
1098901,
1070218,
1118913





]

# Complete list of EMPLIDs to check against
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
'9012314',
# Additon of EMPLIDs where the change in OT treatment has caused a change in the top up amounts - 26/05/25
'1015162',
'9004800',
'1090664',
'9003819',
'9007645',
'1107530',
'1098901',
'9002291',
'1070218',
'1118913'


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

OT_ME_topups = pd.read_parquet(file1)
                             #, index_col=0)
WkdPens_MAlw_CasLoad = pd.read_parquet(file2)
                                     #, index_col=0)



check_emplids(OT_ME_topups, emplids_list, "OT_ME_topups DataFrame")

check_emplids(WkdPens_MAlw_CasLoad, emplids_list, "WkdPens_MAlw_CasLoad DataFrame")
man_invest_indexes = pd.read_excel(man_invest_path)



print("man_invest_indexes duplicates: ")

print(man_invest_indexes['index'].duplicated().sum())  # Should print 0

merged_df = man_invest_indexes.drop_duplicates(subset=['index'], keep='last')


# Define the columns you want to keep
columns_to_keep1 = ['index', 'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 'Pay Date', 'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID', 
                    'Department Name', 'GL_Cost_Account', 'GP_RATE', 'Grade-Step OR Course Code', 'POSITION_NBR', 'Position Title',
                    'REPORTS_TO', 'manual_excl', 'is_student', 'is_perm',
                    'discrepancy_amount_excl', 'cal_shift_top_up', 
                    'three_hour_top_up_cash', 
                   'one_hour_top_up_cash', 'two_hour_top_up_cash']  

# Filter the DataFrame to keep only these columns
OT_ME_topups = OT_ME_topups[columns_to_keep1].fillna(0)






columns_to_keep2 = ['index', 'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 'Pay Date', 'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID', 
                    'Department Name', 'GL_Cost_Account', 'GP_RATE', 'Grade-Step OR Course Code', 'POSITION_NBR', 'Position Title',
                    'REPORTS_TO', 'manual_excl', 
                    'is_student', 'is_perm',
                    'wknd_discrepancy_amount_excl', 'OT_Cas_Loading_Discrp', 'Meal_Allowance']

WkdPens_MAlw_CasLoad = WkdPens_MAlw_CasLoad[columns_to_keep2].fillna(0)


WkdPens_MAlw_CasLoad.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_TestOTCASLOAD{current_date}.csv'), index=False)



OT_ME_topups.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_TestOTME{current_date}.csv'), index=False)


# Merge the dataframes
# merged = pd.merge(OT_ME_topups, WkdPens_MAlw_CasLoad, on=['eFORM_ID', 'NAME', 'EMPLID', 'DATE WORKED', 'BEGINDTTM', 'ENDDTTM'], how='outer')



merged = pd.merge(OT_ME_topups, WkdPens_MAlw_CasLoad, on=['index', 'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 'Pay Date', 'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID', 
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


print('man Invest indexes columns: ')
print(man_invest_indexes.columns)
print('Man Invest indexes head: ')
print(man_invest_indexes.head())

check_emplids(man_invest_indexes, MI_emplids, "Manual Investigation DataFrame")

# Perform a merge on 'index', and bring in only the 'Work Area MI Outcome' column from man_invest_indexes
merged_df = merged.merge(
    man_invest_indexes[['index', 'Work Area MI Outcome', 'Work Area Evidence']], 
    on='index', 
    how='left',  # Left join to keep all rows from merged
    suffixes=('', '_man_invest')
)

# Define the columns to keep
columns_to_keep = [
    'index', 'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 'Pay Date',
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

# Adding No Top Up Cash Step from USC Manual Investigation - 28/02/25
#As per USC's request, if the Work Area MI Outcome is 'No Top Up', set the top up cash columns to 0
merged_df['three_hour_top_up_cash'] = np.where(
    (merged_df['Work Area MI Outcome'] == 'No Top Up'),
    0,
    merged_df['three_hour_top_up_cash'])


merged_df['one_hour_top_up_cash'] = np.where(
    (merged_df['Work Area MI Outcome'] == 'No Top Up'),
    0,
    merged_df['one_hour_top_up_cash'])



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
                'index',  'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 'Pay Date',
                 'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID','Department Name', 'GL_Cost_Account', 'GP_RATE', 'Grade-Step OR Course Code',
                   'POSITION_NBR', 'Position Title', 'REPORTS_TO', 'manual_excl', 'is_student', 'is_perm', 'discrepancy_amount_excl', 'cal_shift_top_up', 
                   'three_hour_top_up_cash', 'one_hour_top_up_cash', 'two_hour_top_up_cash','wknd_discrepancy_amount_excl', 'OT_Cas_Loading_Discrp', 'Meal_Allowance']]


merged_df = merged_df[~merged_df.duplicated(subset=['index'], keep='last')]




cols_to_check = [
    'discrepancy_amount_excl', 'cal_shift_top_up', 'three_hour_top_up_cash',
    'one_hour_top_up_cash', 'two_hour_top_up_cash',
    'wknd_discrepancy_amount_excl', 'OT_Cas_Loading_Discrp', 'Meal_Allowance'
]

merged_df[cols_to_check] = merged_df[cols_to_check].fillna(0)
merged_df = merged_df[~(merged_df[cols_to_check] == 0).all(axis=1)]



merged_df['discrepancy_amount_excl'] = merged_df['discrepancy_amount_excl'].round(2)
merged_df['cal_shift_top_up'] = merged_df['cal_shift_top_up'].round(2)
merged_df['three_hour_top_up_cash'] = merged_df['three_hour_top_up_cash'].round(2)
merged_df['one_hour_top_up_cash'] = merged_df['one_hour_top_up_cash'].round(2)
merged_df['two_hour_top_up_cash'] = merged_df['two_hour_top_up_cash'].round(2)
merged_df['wknd_discrepancy_amount_excl'] = merged_df['wknd_discrepancy_amount_excl'].round(2)
merged_df['OT_Cas_Loading_Discrp'] = merged_df['OT_Cas_Loading_Discrp'].round(2)
merged_df['Meal_Allowance'] = merged_df['Meal_Allowance'].round(2)





prior_infile = [
    '1065200',
'1082447',
'1086737',
'1110567',
'1111375',
'1111577',
'1115571',
'1117164',
'1121038',
'1124461',
'1134550',
'1138183',
'1150609',
'1157420',
'1159456',
'1161781',
'9001610',
'9006461',
'9006535',
'9009265',
'9009308',
'9011752'

]


Changed_EMPLIDs = [
'1015162',
'1070218',
'1084015',
'1090664',
'1095707',
'1098901',
'1107530',
'1117765',
'1118913',
'1126467',
'1132911',
'1140286',
'1150686',
'1155234',
'1164228',
'1166428',
'1167211',
'9002291',
'9003819',
'9004800',
'9007645',
'9009649',
'9010295',
'9011523',
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



merged_df_condensed = merged_df[merged_df['EMPLID'].isin(emplids_list)]

merged_df_condensed.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_merged_condensed{current_date}.csv'), index=False)

check_emplids(merged_df, prior_infile, "22 EMPLIDS in prior file")

check_emplids(merged_df, emplids_list, "all query EMPLIDs")

merged_df.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_merged{current_date}.csv'), index=False)

print(merged_df.columns)



check_emplids(merged_df, emplids_list, "Merged DataFrame")
#Commented out on 19/12/24 Interest Calcs - USC have asked for the interest free outputs






#interest_rates = pd.read_csv(r"c:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\InterestRates.csv")

#print(interest_rates.columns)

#interest_rates['Wages Paid Date'].rename('DATE WORKED', inplace=True)


# print("Missing 'DATE WORKED' in merged_df:", merged_df['DATE WORKED'].isna().sum())
# print("Missing 'DATE WORKED' in interest_rates:", interest_rates['DATE WORKED'].isna().sum())


# with_interest_rate = merged_df.merge(interest_rates, on='DATE WORKED', how='left')

# #Initialise recalc weekend Penalities with Interest Applied column 

# print(with_interest_rate.columns)
# print(with_interest_rate['compInterestFactor'].unique())
# print(with_interest_rate['compInterestFactor'].dtype)
# print(with_interest_rate['compInterestFactor'].head())




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

