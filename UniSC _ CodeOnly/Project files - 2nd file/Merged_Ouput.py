import pandas as pd
import os
import numpy as np
import datetime



output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"
current_date = datetime.datetime.now().strftime('%Y-%m-%d')


#file1 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_OT_ME{current_date}.xlsx')
file1 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_OT_ME2025-03-11.xlsx')

#file2 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_WkdPens_MAlw_CasLoad{current_date}.xlsx')
file2 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_WkdPens_MAlw_CasLoad2025-03-11.xlsx')





OT_ME_topups = pd.read_excel(file1)
                             #, index_col=0)
WkdPens_MAlw_CasLoad = pd.read_excel(file2)
                                     #, index_col=0)




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




merged['discrepancy_amount_excl'] = merged['discrepancy_amount_excl'].round(6)








merged.rename(columns={
    #'index_x': 'OT_ME_topups_Index',
    #'index_y': 'WkdPens_MAlw_CasLoad_Index',
    'Position Title_x': 'Position Title',
    'POSITION_NBR_x': 'POSITION_NBR',
    'is_perm_x': 'is_perm',
    'is_student_x': 'is_student'
}, inplace=True)







# reorder columns
merged = merged[[#'index_Combined', 'OT_ME_topups_Index', 'WkdPens_MAlw_CasLoad_Index',
                'index',  'eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'DATE WORKED', 
                 'PIN_NM', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM', 'DEPTID','Department Name', 'GL_Cost_Account', 'GP_RATE', 'Grade-Step OR Course Code',
                   'POSITION_NBR', 'Position Title', 'REPORTS_TO', 'manual_excl', 'is_student', 'is_perm', 'discrepancy_amount_excl', 'cal_shift_top_up', 
                   'three_hour_top_up_cash', 'one_hour_top_up_cash', 'two_hour_top_up_cash','wknd_discrepancy_amount_excl', 'OT_Cas_Loading_Discrp', 'Meal_Allowance']]


merged = merged[~merged.duplicated(subset=['index'], keep='last')]




# Drop duplicate rows
#merged.drop_duplicates(inplace=True)

# Display the first few rows
print(merged.head())


merged.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_merged{current_date}.csv'), index=False)
# List of columns to sum
# columns_to_sum = [
#     'discrepancy_amount_excl', 'cal_shift_top_up', 'one_hour_top_up_cash',
#     'two_hour_top_up_cash', 'three_hour_top_up_cash', 'wknd_discrepancy_amount_excl',
#     'OT_Cas_Loading_Discrp', 'Meal_Allowance'
# ]

