import pandas as pd
import os

# Define file paths
#enrolment_data_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\UniSC Data Transfer 2 Sept\Student Data Master.xlsx'
enrolment_data_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\UniSC Data Transfer 2 Sept\Student Data Master.xlsx"
enrolment_sheet = 'Enrolment Data'
#timesheet_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_daily_weekly.parquet'
timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_daily_weekly.parquet"
                                                                                                                                                                      

#summary_hr_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\summary_hr_data_permanent.parquet'
summary_hr_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\summary_hr_data_permanent.parquet"



#output_cleaned_data = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\\'
output_cleaned_data =r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"

#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
output_tests = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\'
# Step 1: Load Enrolment Data, keep only necessary columns, and remove duplicates
enrolment_df = pd.read_excel(enrolment_data_path, sheet_name=enrolment_sheet)
enrolment_df = enrolment_df[['Staff Number', 'TERM_BEGIN_DT', 'TERM_END_DT']].drop_duplicates()

# Convert TERM_BEGIN_DT and TERM_END_DT to datetime
enrolment_df['TERM_BEGIN_DT'] = pd.to_datetime(enrolment_df['TERM_BEGIN_DT'])
enrolment_df['TERM_END_DT'] = pd.to_datetime(enrolment_df['TERM_END_DT'])

# Convert 'Staff Number' to string (text)
enrolment_df['Staff Number'] = enrolment_df['Staff Number'].astype(str)

# Step 2: Load timesheet data
timesheet_df = pd.read_parquet(timesheet_path)

# Step 3: Filter timesheet_df to keep only necessary columns and convert DATE WORKED to datetime safely
timesheet_df_filtered = timesheet_df[['EMPLID', 'index', 'DATE WORKED']].copy()  # Use .copy() to avoid view issues

# Convert DATE WORKED to datetime using .loc to avoid SettingWithCopyWarning
timesheet_df_filtered.loc[:, 'DATE WORKED'] = pd.to_datetime(timesheet_df_filtered['DATE WORKED'])

# Step 4: Perform a left join of timesheet and enrolment data based on EMPLID == Staff Number
rows_before_join = len(timesheet_df_filtered)  # Number of rows before the join
joined_df = pd.merge(timesheet_df_filtered, enrolment_df, left_on='EMPLID', right_on='Staff Number', how='left')

# Step 6: Create a flag where DATE WORKED is between TERM_BEGIN_DT and TERM_END_DT
joined_df['is_student'] = ((joined_df['DATE WORKED'] >= joined_df['TERM_BEGIN_DT']) &
                           (joined_df['DATE WORKED'] <= joined_df['TERM_END_DT'])).astype('bool')

# Step 7: Filter out rows where the flag is False and keep only relevant columns
student_df = joined_df[joined_df['is_student']].drop(columns=['Staff Number', 'TERM_BEGIN_DT', 'TERM_END_DT', 'is_student'])

# Step 8: Get the unique list of 'index' and join it back to the original timesheet data
unique_indexes = student_df['index'].unique()
timesheet_df['is_student'] = timesheet_df['index'].isin(unique_indexes)

# Step 9: Save the updated timesheet with student indicator
output_file_parquet = os.path.join(output_cleaned_data, 'timesheet_with_student_indicator.parquet')
timesheet_df.to_parquet(output_file_parquet, index=False)
print(f"Updated timesheet saved with student indicator: {output_file_parquet}")

# Step 10: Sample output where is_student is True
student_sample_df = timesheet_df[timesheet_df['is_student'] == True]

# # Output the sample to an Excel file
# student_sample_output_file = os.path.join(output_tests, 'student_sample_output.xlsx')
# student_sample_df.head(100000).to_excel(student_sample_output_file, index=False)
# print(f"Sample of timesheet data with student indicator ('y') saved to: {student_sample_output_file}")


