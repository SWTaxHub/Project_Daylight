import pandas as pd
import os
import numpy as np
import datetime



output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"
cleaned_data = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"
current_date = datetime.datetime.now().strftime('%Y-%m-%d')


#file1 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_OT_ME{current_date}.xlsx')
file1 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_OT_ME2025-03-05.xlsx')

#file2 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_WkdPens_MAlw_CasLoad{current_date}.xlsx')
file2 = os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_WkdPens_MAlw_CasLoad2025-03-05.xlsx')


man_invest = os.path.join(cleaned_data, f'Manual_Investigation_INDEX_CODES.xlsx')



OT_ME_topups = pd.read_excel(file1)
WkdPens_MAlw_CasLoad = pd.read_excel(file2)

man_invest_indexes = pd.read_excel(man_invest)


print(man_invest_indexes.columns)
print(man_invest_indexes.dtypes)


# Define the columns you want to keep
columns_to_keep1 = ['eFORM_ID', 'NAME', 'EMPLID', 'DATE WORKED', 'BEGINDTTM', 'ENDDTTM', 'discrepancy_amount_excl', 'cal_shift_top_up', 
                    'three_hour_top_up_cash',
                   'one_hour_top_up_cash', 'two_hour_top_up_cash']  

# Filter the DataFrame to keep only these columns
OT_ME_topups = OT_ME_topups[columns_to_keep1].fillna(0)






columns_to_keep2 = ['eFORM_ID', 'NAME', 'EMPLID', 'DATE WORKED', 'BEGINDTTM', 'ENDDTTM', 'wknd_discrepancy_amount_excl', 'OT_Cas_Loading_Discrp', 'Meal_Allowance']

WkdPens_MAlw_CasLoad = WkdPens_MAlw_CasLoad[columns_to_keep2].fillna(0)


WkdPens_MAlw_CasLoad.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_TestOTCASLOAD{current_date}.csv'), index=False)


# Merge the dataframes
merged = pd.merge(OT_ME_topups, WkdPens_MAlw_CasLoad, on=['eFORM_ID', 'NAME', 'EMPLID', 'DATE WORKED', 'BEGINDTTM', 'ENDDTTM'], how='outer')

merged['discrepancy_amount_excl'] = merged['discrepancy_amount_excl'].round(6)


# Drop duplicate rows
#merged.drop_duplicates(inplace=True)

# Display the first few rows
print(merged.head())




# merged = merged.reset_index()


import numpy as np

# Add the index as a column in summed_df
merged['index'] = merged.index  

print('man_invest_indexes.columns: ')
print(man_invest_indexes.columns) 

print(merged['index'].dtype)
print(man_invest_indexes['index'].dtype)



# Perform a merge on 'index', and bring in only the 'Work Area MI Outcome' column from man_invest_indexes
merged_df = merged.merge(man_invest_indexes[['index', 'Work Area MI Outcome']], 
                             on='index', 
                             how='left',  # Left join to keep all rows from summed_df
                             suffixes=('', '_man_invest'))


print(merged_df.columns)
# Apply the condition using np.where
merged_df['discrepancy_amount_excl'] = np.where(
    (merged_df['Work Area MI Outcome'] == 'OT NOT PAYABLE'),  # Condition on 'Work Area MI Outcome'
    0,  # Set to 0 if condition is true
    merged_df['discrepancy_amount_excl']  # Else, keep the original value
)

# Now, update summed_df with the modified 'discrepancy_amount_excl'
merged['discrepancy_amount_excl'] = merged_df['discrepancy_amount_excl']









merged.to_csv(os.path.join(output_tests, f'underpayment_sample_transactions_with_topups_merged{current_date}.csv'), index=False)
# List of columns to sum
columns_to_sum = [
    'discrepancy_amount_excl', 'cal_shift_top_up', 'one_hour_top_up_cash',
    'two_hour_top_up_cash', 'three_hour_top_up_cash', 'wknd_discrepancy_amount_excl',
    'OT_Cas_Loading_Discrp', 'Meal_Allowance'
]

# Group by EMPLID and sum the specified columns
summed_df = merged.groupby('EMPLID', as_index=False)[columns_to_sum].sum()
#summed_df = merged.groupby('EMPLID')[columns_to_sum].sum()
# Display the result
print(summed_df.head())


# if the index is in man_invest_indexes and in summed_df and Work Area MI Outcome == 'OT NOT PAYABLE' then set 
# the discrepancy_amount_excl to 0

# import numpy as np

# summed_df['discrepancy_amount_excl'] = np.where(
#     (summed_df.index.isin(man_invest_indexes)) & 
#     (man_invest_indexes['Work Area MI Outcome'] == 'OT NOT PAYABLE'),
#     0,
#     summed_df['discrepancy_amount_excl']
# )

# import numpy as np

summed_df = summed_df.reset_index()

print('summed_df.columns: ')
print(summed_df.columns)

import numpy as np

# Add the index as a column in summed_df
summed_df['index'] = summed_df.index  

print('man_invest_indexes.columns: ')
print(man_invest_indexes.columns) 

print(summed_df['index'].dtype)
print(man_invest_indexes['index'].dtype)



# Perform a merge on 'index', and bring in only the 'Work Area MI Outcome' column from man_invest_indexes
merged_df = summed_df.merge(man_invest_indexes[['index', 'Work Area MI Outcome']], 
                             on='index', 
                             how='left',  # Left join to keep all rows from summed_df
                             suffixes=('', '_man_invest'))


print(merged_df.columns)
# Apply the condition using np.where
merged_df['discrepancy_amount_excl'] = np.where(
    (merged_df['Work Area MI Outcome'] == 'OT NOT PAYABLE'),  # Condition on 'Work Area MI Outcome'
    0,  # Set to 0 if condition is true
    merged_df['discrepancy_amount_excl']  # Else, keep the original value
)

# Now, update summed_df with the modified 'discrepancy_amount_excl'
summed_df['discrepancy_amount_excl'] = merged_df['discrepancy_amount_excl']



# if the index is in man_invest_indexes and in summed_df and Work Area MI Outcome == '1 hour' then set
# the three_hour_top_up_cash to 0



# Save the merged DataFrame to a new Excel file
output_file = os.path.join(output_tests, f'underpayment_sample_transactions_SummaryByEMPLID{current_date}.xlsx')
summed_df.head(100000).to_excel(output_file, index=False)