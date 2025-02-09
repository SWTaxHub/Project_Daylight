import os
from os import listdir
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('ggplot')
import math
import statistics


# Change to the desired directory
#new_directory = r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet'
#os.chdir(new_directory)


#Merge Dataframes with pandas


files = [file for file in os.listdir(r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV')]

all_years_data = pd.DataFrame()


for file in files:
    
   grades_df = pd.read_csv(os.path.join(r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV', file), encoding='latin1', low_memory=False)
   all_years_data = pd.concat([all_years_data, grades_df], ignore_index=True)  # Use ignore_index=True to reset index


all_years_data = all_years_data.rename(columns={'Course ID' : 'Course_ID', 'Course Name' : 'Course_Name', 'Grade Centre Title' : 'Grade_Centre_Title',
       'Staff ID' : 'Staff_ID', 'Marker Username' : 'Marker_Username', 'Marker Firstname' : 'Marker_Username',
       'Marker Lastname' : 'Marker_Lastname', 'Latest Attempt' : 'Latest_Attempt', 'Grade Centre Identifier' : 'Grade_Centre_ID',
       'Part of Total Interim (Yes/No) ' : 'partOfTotalInterim', 'Weight of Column' : 'Weight_of_Column' })


all_years_data['Staff_ID'] = all_years_data['Staff_ID'].apply(str)
all_years_data['Course_ID'] = all_years_data['Course_ID'].apply(str)
all_years_data['Latest_Attempt'] = pd.to_datetime(all_years_data['Latest_Attempt'],   format='%d/%m/%Y', 
                                                    errors='coerce')

print(all_years_data.value_counts)

print(all_years_data.shape)


#print(all_years_data.head())
#print(all_years_data.dtypes)



# Now we want to group by 'Course_ID' and 'Student ID' (assuming Student ID is a column) and sum 'Weight_of_Column'
# Let's ensure 'Student ID' exists or define it if needed
if 'Student' not in all_years_data.columns:
    print("Error: 'Student' column is missing.")
else:
    # Group by 'Course_ID' and 'Student_ID' and sum 'Weight_of_Column'
    result = all_years_data.groupby(['Course_ID', 'Course_Name','Staff_ID','Student'])['Weight_of_Column'].sum().reset_index()




    # Apply the condition that if the total sum exceeds 100, limit it to 100
    result['Weight_of_Column'] = result['Weight_of_Column'].apply(lambda x: min(x, 100))



    TotalWeightbyCourseStaff = result.groupby(['Course_ID', 'Course_Name', 'Staff_ID'])['Weight_of_Column'].sum().reset_index()

    
    TotalWeightbyCourseStaff['Weight_of_Column'] = TotalWeightbyCourseStaff['Weight_of_Column']/100



    # Now, if you need to see the output, you can print or inspect the result
    print(TotalWeightbyCourseStaff.head())



#result.to_csv("StudentWeightTotals.    ` csv", index=False)

TotalWeightbyCourseStaff.to_csv("StudentWeightTotals.csv", index=False)




all_years_data.to_csv("all_data.csv", index=False)


all_data = all_years_data.copy()

print("BB data shape")
print(all_data.shape)




print("Count of Course IDs in BB")
print(len(all_data['Course_ID'].unique()))

print("Count of Staff_ID in BB")
print(len(all_data['Staff_ID'].unique()))


# Assuming 'all_data' is your DataFrame
# Filter numeric values in the 'Staff_ID' column
numeric_staff_ids = all_data['Staff_ID'][all_data['Staff_ID'].apply(lambda x: str(x).isdigit())]

# Count unique numeric values
numeric_unique_count = numeric_staff_ids.nunique()

print("Count of numeric Staff_ID in BB:")
print(numeric_unique_count)




# Display unique combinations of concatenated_GradeStepTerm and EMPLID
unique_combinations = all_data[['Course_ID', 'Staff_ID']].drop_duplicates()

# Print the count of unique combinations
print("Count of unique Course and Staff ID combinations in BB:")
print(len(unique_combinations))

# Display the unique combinations (if needed)
print("Unique combinations of Course and Staff ID:")
print(unique_combinations)



#print('Data Types in all years data')
#print(all_years_data.dtypes)
#print(all_years_data['Staff_ID'].unique())
#print(all_years_data['Course_ID'].unique())
#print(all_years_data.isnull().sum())


# Group the data by 'Course_ID', 'Course_Name', and 'Staff_ID', 
# and count the number of unique student IDs ('Student') for each group. 
# The result is a DataFrame containing the unique student count 
# for each staff member associated with each course.
StudentsCourse = all_years_data.groupby(['Course_ID', 'Course_Name', 'Staff_ID'])['Student'].nunique().reset_index(name='Unique_Student_Count')





#.reset_index(name='Unique_Student_Count') 

StudentsCourse['Staff_ID'] = StudentsCourse['Staff_ID'].apply(str)
StudentsCourse['Course_ID'] = StudentsCourse['Course_ID'].apply(str)





StudentsCourse.to_csv("all_dataByStudentsInCourse.csv")


MergedStudentCountandWeight = TotalWeightbyCourseStaff.merge(StudentsCourse, on=['Staff_ID',  'Course_Name', 'Course_ID'], how='left')

print(MergedStudentCountandWeight.head())


MergedStudentCountandWeight.to_csv("StudentCountandWeightsCompared.csv")


#Timesheet Data Frame

df = pd.read_csv(
    r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Timesheet_TermID_df.csv',
    encoding='latin1',
    #dtype={'Column_Name': str}  
)



#print df.columns and then copy and paste names of the columns like below use # to comment out the values we don't want
Ndf = df[['EMPLID', 
          'DATE WORKED', 
          'Pay Code', 'UNITS_CLAIMED',
       'Grade-Step OR Course Code', 
       #'Index', 
       'TERM ID', 'eFORM_ID', 'NAME',
       #'EMPL_RCD', 'G3FORM_CONDITION', 'G3FORM_STATUS',
       #'Claimed Period Begin Date', 'Claimed Period End Date', 
       #'CAL_PRD_ID',
       #'Pay Date', 
       #'G_START_AM_PM', 'G_START_HOUR', 'G_START_MINUTE',
       #'G_FINISH_AM_PM', 'G_FINISH_HOUR', 'G_FINISH_MINUTE', 'G_BREAK_MINUTES',
       #'G_ELAPSED_HOURS_WORKED', 'G_ELAPSED_MINUTES_WORKED', 'BEGINDTTM',
       #'ENDDTTM',
       'DEPTID', 'Department Name', 'GL_Cost_Account', 'JOBCODE',
       'FULL_PART_TIME', 
       #'GP_RATE', 'Fortnight End', 
       'POSITION_NBR',
       'Position Title', 'REPORTS_TO', 
       #'Report Run Date', 'Day of week',
       #'Weekend Penalty', '> 22/11/2023 Span of Hours', 'Column40'
       ]].copy()




#Rename columns to remove spaces
Ndf = Ndf.rename(columns={'DATE WORKED' : 'DATE_WORKED', 'Pay Code' : 'Pay_Code', 'Grade-Step OR Course Code' : 'GradeStep_Or_CourseCode', 'TERM ID' : 'TERM_ID', 
                         'Department Name' : 'Department_Name', 'Position Title' : 'Position_Title'})

Ndf.columns = Ndf.columns.str.strip()

Ndf['DATE_WORKED'] = pd.to_datetime(Ndf['DATE_WORKED'],   
#format='%d/%m/%Y', 
                                                    errors='coerce')






start_date = '1/09/2016'

end_date = '31/12/2022'

# Convert the start and end dates to datetime objects
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)


# Filter the DataFrame based on the date range
datefiltered_df = Ndf[Ndf['DATE_WORKED'].between(start_date, end_date)]







Ndf_amended = datefiltered_df.copy()

Ndf_amended['concatenated_GradeStepTerm'] = (
    Ndf_amended['GradeStep_Or_CourseCode'] + '-' + Ndf_amended['TERM_ID'].astype(str)
)


print("Count of Course IDs in Timesheet")
print(len(Ndf_amended['concatenated_GradeStepTerm'].unique()))

print("Count of EMPLIDs in Timesheet")
print(len(Ndf_amended['EMPLID'].unique()))


# Display unique combinations of concatenated_GradeStepTerm and EMPLID
unique_combinations2 = Ndf_amended[['concatenated_GradeStepTerm', 'EMPLID']].drop_duplicates()

# Print the count of unique combinations
print("Count of unique Course and Staff ID combinations in Timesheet:")
print(len(unique_combinations2))

# Display the unique combinations (if needed)
print("Unique combinations of Course and Staff ID:")
print(unique_combinations2)




'''
output_file_NdfAmendedTest = 'NdfAmendedTest_dataset.csv'
Ndf_amended.to_csv(output_file_NdfAmendedTest, index=False)

'''

sorted_Ndf = Ndf_amended.sort_values(by='concatenated_GradeStepTerm')




Unfiltered_Ndf = sorted_Ndf

Unfiltered_Ndf.to_csv('UnfilteredTimesheet.csv')


# Filter for rows where JOBCODE is "CASUAL" and FULL_PART_TIME is either "NULL" or "D"
filtered_Ndf = sorted_Ndf[
    (sorted_Ndf['JOBCODE'] != "CASUAL") & 
    (sorted_Ndf['FULL_PART_TIME'].isin(["NULL", "D"]))
]

inverse_filtered_Ndf = sorted_Ndf[
    (sorted_Ndf['JOBCODE'] == "CASUAL") | 
    (~sorted_Ndf['FULL_PART_TIME'].isin(["NULL", "D"]))
]



#output_file_filtered = 'filtered_amended_dataset.csv'
#filtered_Ndf.to_csv(output_file_filtered, index=False)

# Save the Inverse of the filtered DataFrame to a new CSV file so we know what has been excluded
output_file_Inversefiltered = 'Inversefiltered_amended_dataset.csv'
inverse_filtered_Ndf.to_csv(output_file_Inversefiltered, index=False)



# Drop rows where GradeStep_Or_CourseCode is NaN, blank, or just spaces
filtered2_Ndf = filtered_Ndf[
    filtered_Ndf['GradeStep_Or_CourseCode'].notna() & 
    (filtered_Ndf['GradeStep_Or_CourseCode'] != '') & 
    (filtered_Ndf['GradeStep_Or_CourseCode'] != ' ')
]


Inversefiltered2_Ndf = filtered_Ndf[
    filtered_Ndf['GradeStep_Or_CourseCode'].isna() | 
    (filtered_Ndf['GradeStep_Or_CourseCode'] == '') | 
    (filtered_Ndf['GradeStep_Or_CourseCode'] == ' ')
]


# Display unique combinations of concatenated_GradeStepTerm and EMPLID
unique_combinations3 = filtered2_Ndf[['concatenated_GradeStepTerm', 'EMPLID']].drop_duplicates()


# Print the count of unique combinations
print("Count of unique Course and Staff ID combinations in Timesheet Filtered2:")
print(len(unique_combinations3))

output_Inversefiltered2_Ndf = 'Inversefiltered2_amended_dataset.csv'
Inversefiltered2_Ndf.to_csv(output_Inversefiltered2_Ndf, index=False)






# Save the filtered DataFrame to a new CSV file
output_file_filtered = 'filtered2_amended_dataset.csv'
filtered2_Ndf.to_csv(output_file_filtered, index=False)

# Print a message to confirm
print(f"Filtered amended dataset saved to {output_file_filtered}")


# Get unique EMPLID values grouped by concatenated_GradeStepTerm
unique_emplids = filtered2_Ndf.groupby(['concatenated_GradeStepTerm', 'Department_Name', 'Position_Title', 'Pay_Code', 'UNITS_CLAIMED'])['EMPLID'].unique().reset_index()

# Explode the unique EMPLID lists to have each EMPLID in its own row
exploded_emplids = unique_emplids.explode('EMPLID').reset_index(drop=True)


#print('unique_emplids head')
#print(unique_emplids.head())


#print('Exploded_emplids head')
#print(exploded_emplids.head())
# Save to a CSV file
output_file_exploded_emplids = 'exploded_emplids_by_grade_step_term.csv'
exploded_emplids.to_csv(output_file_exploded_emplids, index=False)





# Create a new DataFrame that includes the relevant columns for each EMPLID
emplid_units = filtered2_Ndf[['EMPLID', 'concatenated_GradeStepTerm', 'Department_Name', 'Position_Title', 'Pay_Code', 'UNITS_CLAIMED']]



# This will automatically give each EMPLID its own row along with the relevant information
# If you want to ensure the UNITS_CLAIMED is summed for the same EMPLID, you can group by the EMPLID and the other columns

# Group by EMPLID and the other relevant columns, summing the UNITS_CLAIMED
unique_emplids = emplid_units.groupby(
    ['EMPLID', 'concatenated_GradeStepTerm', 'Department_Name', 'Position_Title', 'Pay_Code']
)['UNITS_CLAIMED'].sum().reset_index()


unique_emplids['concatenated_GradeStepTerm'] = unique_emplids['concatenated_GradeStepTerm'].apply(str)
unique_emplids['EMPLID'] = unique_emplids['EMPLID'].apply(str)

#This is the file that presents the SUM of claimed hours by employee for each Module within each Semester
# Specify the output file path
output_file = 'emplid_units_summary.csv'

# Write the result to a CSV file
unique_emplids.to_csv(output_file, index=False)


# Group by 'EMPLID' and 'concatenated_GradeStepTerm', then sum 'UNITS_CLAIMED'
summed_units = unique_emplids.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()




summed_units = summed_units.rename(columns={'UNITS_CLAIMED': 'Total_Units_Claimed' })


unique_combinations4 = summed_units[[ 'EMPLID', 'concatenated_GradeStepTerm']]


# Print the count of unique combinations
print("Count of unique Course and Staff ID combinations in Timesheet summed_units:")
print(len(unique_combinations4))


# Define the desired Pay_Codes
desired_pay_codes_MARKING = ['MARKING']

desired_payCode_MARKING2 = ['MARKING2']
desired_payCode_MARKSUPVR = ['MARKSUPVR']
desired_pay_codes_OTHERACT2 = ['OTHERACT2']
desired_pay_codes_OTHERACTIV = ['OTHERACTIV']
desired_pay_codes_CLINFAC = ['CLINFAC']
desired_pay_codes_TUTORING = ['TUTORING']
desired_pay_codes_LECTBASIC = ['LECTBASIC']
desired_pay_codes_TRCASUAL = ['TRCASUAL']
desired_pay_codes_TUTORRPT2 = ['TUTORRPT2']
desired_pay_codes_TUTORRPT = ['TUTORRPT']
desired_pay_codes_TUTORSTP2 = ['TUTORSTP2']
desired_pay_codes_LECTRPT = ['LECTRPT']
desired_pay_codes_MISCDUTIES = ['MISCDUTIES']
desired_pay_codes_LECTSPEC = ['LECTSPEC']
desired_pay_codes_MISCDUT2 = ['MISCDUT2']
desired_pay_codes_LECTDEVEL = ['LECTDEVEL']
desired_pay_codes_CLINSUN = ['CLINSUN']
desired_pay_codes_OT = ['OT']
desired_pay_codes_SATCASUAL = ['SATCASUAL']
desired_pay_codes_CLINSAT = ['CLINSAT']
desired_pay_codes_SUNCASUAL = ['SUNCASUAL']
desired_pay_codes_ADDIT = ['ADDIT']
desired_pay_codes_SESSDAY = ['SESSDAY']



'''
# Unfiltered  DataFrame to include only the specified Pay_Codes




# Group by 'EMPLID' and 'concatenated_GradeStepTerm', then sum 'UNITS_CLAIMED'
summed_units2  = Unfiltered_Ndf.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()

summed_units2 = summed_units.rename(columns={'UNITS_CLAIMED': 'Total_Units_Claimed' })


unfiltered_data = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_MARKING)]

unfiltered_data2 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_payCode_MARKING2)]

unfiltered_data3 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_payCode_MARKSUPVR)]

unfiltered_data4 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_OTHERACT2)]
unfiltered_data5 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_OTHERACTIV)]
unfiltered_data6 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_CLINFAC)]
unfiltered_data7 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_TUTORING)]
unfiltered_data8 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_LECTBASIC)]
unfiltered_data9 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_TRCASUAL)]
unfiltered_data10 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_TUTORRPT2)]
unfiltered_data11 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_TUTORRPT)]
unfiltered_data12 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_TUTORSTP2)]
unfiltered_data13 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_LECTRPT)]
unfiltered_data14 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_MISCDUTIES)]
unfiltered_data15 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_LECTSPEC)]
unfiltered_data16 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_MISCDUT2)]
unfiltered_data17 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_LECTDEVEL)]
unfiltered_data18 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_CLINSUN)]
unfiltered_data19 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_OT)]
unfiltered_data20 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_SATCASUAL)]
unfiltered_data21 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_CLINSAT)]
unfiltered_data22 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_SUNCASUAL)]
unfiltered_data23 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_ADDIT)]
unfiltered_data24 = Unfiltered_Ndf[Unfiltered_Ndf['Pay_Code'].isin(desired_pay_codes_SESSDAY)]


# Step 2: Group by and sum UNITS_CLAIMED for the specified pay codes
paycode_sums = unfiltered_data.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()

paycode_sums = paycode_sums.rename(columns={'UNITS_CLAIMED': 'Marking_Units_Claimed' })


paycode_sums2 = unfiltered_data2.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()

paycode_sums2 = paycode_sums2.rename(columns={'UNITS_CLAIMED': 'MARKING2_Units_Claimed' })


paycode_sums3 = unfiltered_data3.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()

paycode_sums3 = paycode_sums3.rename(columns={'UNITS_CLAIMED': 'MARKSUPVR_Units_Claimed' })

paycode_sum4 = unfiltered_data4.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum5 = unfiltered_data5.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum6 = unfiltered_data6.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum7 = unfiltered_data7.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum8 = unfiltered_data8.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum9 = unfiltered_data9.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum10 = unfiltered_data10.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum11 = unfiltered_data11.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum12 = unfiltered_data12.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum13 = unfiltered_data13.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum14 = unfiltered_data14.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum15 = unfiltered_data15.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum16 = unfiltered_data16.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum17 = unfiltered_data17.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum18 = unfiltered_data18.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum19 = unfiltered_data19.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum20 = unfiltered_data20.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum21 = unfiltered_data21.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum22 = unfiltered_data22.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum23 = unfiltered_data23.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum24 = unfiltered_data24.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()



paycode_sum4 = paycode_sum4.rename(columns={'UNITS_CLAIMED':  'OTHERACT2_Units_Claimed' })
paycode_sums5 = paycode_sum5.rename(columns={'UNITS_CLAIMED':  'OTHERACTIV _Units_Claimed' })
paycode_sums6 = paycode_sum6.rename(columns={'UNITS_CLAIMED':  'CLINFAC_Units_Claimed' })
paycode_sum7 = paycode_sum7.rename(columns={'UNITS_CLAIMED':  'TUTORING_Units_Claimed' })
paycode_sum8 = paycode_sum8.rename(columns={'UNITS_CLAIMED':  'LECTBASIC_Units_Claimed' })
paycode_sum9 = paycode_sum9.rename(columns={'UNITS_CLAIMED':  'TRCASUAL_Units_Claimed' })
paycode_sum10 = paycode_sum10.rename(columns={'UNITS_CLAIMED':  'TUTORRPT2_Units_Claimed' })
paycode_sum11 = paycode_sum11.rename(columns={'UNITS_CLAIMED':  'TUTORRPT_Units_Claimed' })
paycode_sum12 = paycode_sum12.rename(columns={'UNITS_CLAIMED':  'TUTORSTP2_Units_Claimed' })
paycode_sum13 = paycode_sum13.rename(columns={'UNITS_CLAIMED':  'LECTRPT_Units_Claimed' })
paycode_sum14 = paycode_sum14.rename(columns={'UNITS_CLAIMED':  'MISCDUTIES_Units_Claimed' })
paycode_sum15 = paycode_sum15.rename(columns={'UNITS_CLAIMED':  'LECTSPEC_Units_Claimed' })
paycode_sum16 = paycode_sum16.rename(columns={'UNITS_CLAIMED':  'MISCDUT2_Units_Claimed' })
paycode_sum17 = paycode_sum17.rename(columns={'UNITS_CLAIMED':  'LECTDEVEL_Units_Claimed' })
paycode_sum18 = paycode_sum18.rename(columns={'UNITS_CLAIMED':  'CLINSUN_Units_Claimed' })
paycode_sum19 = paycode_sum19.rename(columns={'UNITS_CLAIMED':  'OT_Units_Claimed' })
paycode_sum20 = paycode_sum20.rename(columns={'UNITS_CLAIMED':  'SATCASUAL_Units_Claimed' })
paycode_sum21 = paycode_sum21.rename(columns={'UNITS_CLAIMED':  'CLINSAT_Units_Claimed' })
paycode_sum22 = paycode_sum22.rename(columns={'UNITS_CLAIMED':  'SUNCASUAL_Units_Claimed' })
paycode_sum23 = paycode_sum23.rename(columns={'UNITS_CLAIMED':  'ADDIT_Units_Claimed' })
paycode_sum24 = paycode_sum24.rename(columns={'UNITS_CLAIMED':  'SESSDAY_Units_Claimed' })




# Ensure both EMPLID columns are of the same type (string in this case)
summed_units['EMPLID'] = summed_units['EMPLID'].astype(str)
paycode_sums['EMPLID'] = paycode_sums['EMPLID'].astype(str)
paycode_sums2['EMPLID'] = paycode_sums2['EMPLID'].astype(str)
paycode_sums3['EMPLID'] = paycode_sums3['EMPLID'].astype(str)
paycode_sum4['EMPLID'] = paycode_sum4['EMPLID'].astype(str)
paycode_sum5['EMPLID'] = paycode_sum5['EMPLID'].astype(str)
paycode_sum6['EMPLID'] = paycode_sum6['EMPLID'].astype(str)
paycode_sum7['EMPLID'] = paycode_sum7['EMPLID'].astype(str)
paycode_sum8['EMPLID'] = paycode_sum8['EMPLID'].astype(str)
paycode_sum9['EMPLID'] = paycode_sum9['EMPLID'].astype(str)
paycode_sum10['EMPLID'] = paycode_sum10['EMPLID'].astype(str)
paycode_sum11['EMPLID'] = paycode_sum11['EMPLID'].astype(str)
paycode_sum12['EMPLID'] = paycode_sum12['EMPLID'].astype(str)
paycode_sum13['EMPLID'] = paycode_sum13['EMPLID'].astype(str)
paycode_sum14['EMPLID'] = paycode_sum14['EMPLID'].astype(str)
paycode_sum15['EMPLID'] = paycode_sum15['EMPLID'].astype(str)
paycode_sum16['EMPLID'] = paycode_sum16['EMPLID'].astype(str)
paycode_sum17['EMPLID'] = paycode_sum17['EMPLID'].astype(str)
paycode_sum18['EMPLID'] = paycode_sum18['EMPLID'].astype(str)
paycode_sum19['EMPLID'] = paycode_sum19['EMPLID'].astype(str)
paycode_sum20['EMPLID'] = paycode_sum20['EMPLID'].astype(str)
paycode_sum21['EMPLID'] = paycode_sum21['EMPLID'].astype(str)
paycode_sum22['EMPLID'] = paycode_sum22['EMPLID'].astype(str)
paycode_sum23['EMPLID'] = paycode_sum23['EMPLID'].astype(str)
paycode_sum24['EMPLID'] = paycode_sum24['EMPLID'].astype(str)

# Step 3: Merge the summed amounts back into the original DataFrame
SumMerged_df1 = summed_units2.merge(paycode_sums, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')



SumMerged_df2 = SumMerged_df1.merge(paycode_sums2, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')


SumMerged_df3 = SumMerged_df2.merge(paycode_sums3, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df4 = SumMerged_df3.merge(paycode_sum4, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df5 = SumMerged_df4.merge(paycode_sum5, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df6 = SumMerged_df5.merge(paycode_sum6, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df7 = SumMerged_df6.merge(paycode_sum7, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df8 = SumMerged_df7.merge(paycode_sum8, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df9 = SumMerged_df8.merge(paycode_sum9, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df10 = SumMerged_df9.merge(paycode_sum10, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df11 = SumMerged_df10.merge(paycode_sum11, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df12 = SumMerged_df11.merge(paycode_sum12, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df13 = SumMerged_df12.merge(paycode_sum13, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df14 = SumMerged_df13.merge(paycode_sum14, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df15 = SumMerged_df14.merge(paycode_sum15, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df16 = SumMerged_df15.merge(paycode_sum16, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df17 = SumMerged_df16.merge(paycode_sum17, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df18 = SumMerged_df17.merge(paycode_sum18, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df19 = SumMerged_df18.merge(paycode_sum19, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df20 = SumMerged_df19.merge(paycode_sum20, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df21 = SumMerged_df20.merge(paycode_sum21, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df22 = SumMerged_df21.merge(paycode_sum22, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df23 = SumMerged_df22.merge(paycode_sum23, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')




SumMerged_df = SumMerged_df23.merge(paycode_sum24, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')


SumMerged_df.to_csv('UnfilteredTimesheetDF.csv')
'''



# Filter the DataFrame to include only the specified Pay_Codes
filtered_data = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_MARKING)]

filtered_data2 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_payCode_MARKING2)]

filtered_data3 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_payCode_MARKSUPVR)]

filtered_data4 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_OTHERACT2)]
filtered_data5 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_OTHERACTIV)]
filtered_data6 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_CLINFAC)]
filtered_data7 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_TUTORING)]
filtered_data8 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_LECTBASIC)]
filtered_data9 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_TRCASUAL)]
filtered_data10 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_TUTORRPT2)]
filtered_data11 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_TUTORRPT)]
filtered_data12 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_TUTORSTP2)]
filtered_data13 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_LECTRPT)]
filtered_data14 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_MISCDUTIES)]
filtered_data15 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_LECTSPEC)]
filtered_data16 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_MISCDUT2)]
filtered_data17 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_LECTDEVEL)]
filtered_data18 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_CLINSUN)]
filtered_data19 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_OT)]
filtered_data20 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_SATCASUAL)]
filtered_data21 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_CLINSAT)]
filtered_data22 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_SUNCASUAL)]
filtered_data23 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_ADDIT)]
filtered_data24 = filtered2_Ndf[filtered2_Ndf['Pay_Code'].isin(desired_pay_codes_SESSDAY)]







# Step 2: Group by and sum UNITS_CLAIMED for the specified pay codes
paycode_sums = filtered_data.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()

paycode_sums = paycode_sums.rename(columns={'UNITS_CLAIMED': 'Marking_Units_Claimed' })


paycode_sums2 = filtered_data2.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()

paycode_sums2 = paycode_sums2.rename(columns={'UNITS_CLAIMED': 'MARKING2_Units_Claimed' })


paycode_sums3 = filtered_data3.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()

paycode_sums3 = paycode_sums3.rename(columns={'UNITS_CLAIMED': 'MARKSUPVR_Units_Claimed' })

paycode_sum4 = filtered_data4.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum5 = filtered_data5.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum6 = filtered_data6.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum7 = filtered_data7.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum8 = filtered_data8.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum9 = filtered_data9.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum10 = filtered_data10.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum11 = filtered_data11.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum12 = filtered_data12.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum13 = filtered_data13.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum14 = filtered_data14.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum15 = filtered_data15.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum16 = filtered_data16.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum17 = filtered_data17.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum18 = filtered_data18.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum19 = filtered_data19.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum20 = filtered_data20.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum21 = filtered_data21.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum22 = filtered_data22.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum23 = filtered_data23.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()
paycode_sum24 = filtered_data24.groupby(['EMPLID', 'concatenated_GradeStepTerm'], as_index=False)['UNITS_CLAIMED'].sum()



paycode_sum4 = paycode_sum4.rename(columns={'UNITS_CLAIMED':  'OTHERACT2_Units_Claimed' })
paycode_sums5 = paycode_sum5.rename(columns={'UNITS_CLAIMED':  'OTHERACTIV _Units_Claimed' })
paycode_sums6 = paycode_sum6.rename(columns={'UNITS_CLAIMED':  'CLINFAC_Units_Claimed' })
paycode_sum7 = paycode_sum7.rename(columns={'UNITS_CLAIMED':  'TUTORING_Units_Claimed' })
paycode_sum8 = paycode_sum8.rename(columns={'UNITS_CLAIMED':  'LECTBASIC_Units_Claimed' })
paycode_sum9 = paycode_sum9.rename(columns={'UNITS_CLAIMED':  'TRCASUAL_Units_Claimed' })
paycode_sum10 = paycode_sum10.rename(columns={'UNITS_CLAIMED':  'TUTORRPT2_Units_Claimed' })
paycode_sum11 = paycode_sum11.rename(columns={'UNITS_CLAIMED':  'TUTORRPT_Units_Claimed' })
paycode_sum12 = paycode_sum12.rename(columns={'UNITS_CLAIMED':  'TUTORSTP2_Units_Claimed' })
paycode_sum13 = paycode_sum13.rename(columns={'UNITS_CLAIMED':  'LECTRPT_Units_Claimed' })
paycode_sum14 = paycode_sum14.rename(columns={'UNITS_CLAIMED':  'MISCDUTIES_Units_Claimed' })
paycode_sum15 = paycode_sum15.rename(columns={'UNITS_CLAIMED':  'LECTSPEC_Units_Claimed' })
paycode_sum16 = paycode_sum16.rename(columns={'UNITS_CLAIMED':  'MISCDUT2_Units_Claimed' })
paycode_sum17 = paycode_sum17.rename(columns={'UNITS_CLAIMED':  'LECTDEVEL_Units_Claimed' })
paycode_sum18 = paycode_sum18.rename(columns={'UNITS_CLAIMED':  'CLINSUN_Units_Claimed' })
paycode_sum19 = paycode_sum19.rename(columns={'UNITS_CLAIMED':  'OT_Units_Claimed' })
paycode_sum20 = paycode_sum20.rename(columns={'UNITS_CLAIMED':  'SATCASUAL_Units_Claimed' })
paycode_sum21 = paycode_sum21.rename(columns={'UNITS_CLAIMED':  'CLINSAT_Units_Claimed' })
paycode_sum22 = paycode_sum22.rename(columns={'UNITS_CLAIMED':  'SUNCASUAL_Units_Claimed' })
paycode_sum23 = paycode_sum23.rename(columns={'UNITS_CLAIMED':  'ADDIT_Units_Claimed' })
paycode_sum24 = paycode_sum24.rename(columns={'UNITS_CLAIMED':  'SESSDAY_Units_Claimed' })



# Rename the summed column to 'AMOUNT'

# Ensure both EMPLID columns are of the same type (string in this case)
summed_units['EMPLID'] = summed_units['EMPLID'].astype(str)
paycode_sums['EMPLID'] = paycode_sums['EMPLID'].astype(str)
paycode_sums2['EMPLID'] = paycode_sums2['EMPLID'].astype(str)
paycode_sums3['EMPLID'] = paycode_sums3['EMPLID'].astype(str)
paycode_sum4['EMPLID'] = paycode_sum4['EMPLID'].astype(str)
paycode_sum5['EMPLID'] = paycode_sum5['EMPLID'].astype(str)
paycode_sum6['EMPLID'] = paycode_sum6['EMPLID'].astype(str)
paycode_sum7['EMPLID'] = paycode_sum7['EMPLID'].astype(str)
paycode_sum8['EMPLID'] = paycode_sum8['EMPLID'].astype(str)
paycode_sum9['EMPLID'] = paycode_sum9['EMPLID'].astype(str)
paycode_sum10['EMPLID'] = paycode_sum10['EMPLID'].astype(str)
paycode_sum11['EMPLID'] = paycode_sum11['EMPLID'].astype(str)
paycode_sum12['EMPLID'] = paycode_sum12['EMPLID'].astype(str)
paycode_sum13['EMPLID'] = paycode_sum13['EMPLID'].astype(str)
paycode_sum14['EMPLID'] = paycode_sum14['EMPLID'].astype(str)
paycode_sum15['EMPLID'] = paycode_sum15['EMPLID'].astype(str)
paycode_sum16['EMPLID'] = paycode_sum16['EMPLID'].astype(str)
paycode_sum17['EMPLID'] = paycode_sum17['EMPLID'].astype(str)
paycode_sum18['EMPLID'] = paycode_sum18['EMPLID'].astype(str)
paycode_sum19['EMPLID'] = paycode_sum19['EMPLID'].astype(str)
paycode_sum20['EMPLID'] = paycode_sum20['EMPLID'].astype(str)
paycode_sum21['EMPLID'] = paycode_sum21['EMPLID'].astype(str)
paycode_sum22['EMPLID'] = paycode_sum22['EMPLID'].astype(str)
paycode_sum23['EMPLID'] = paycode_sum23['EMPLID'].astype(str)
paycode_sum24['EMPLID'] = paycode_sum24['EMPLID'].astype(str)


# Step 3: Merge the summed amounts back into the original DataFrame
SumMerged_df1 = summed_units.merge(paycode_sums, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')



SumMerged_df2 = SumMerged_df1.merge(paycode_sums2, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')


SumMerged_df3 = SumMerged_df2.merge(paycode_sums3, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df4 = SumMerged_df3.merge(paycode_sum4, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df5 = SumMerged_df4.merge(paycode_sum5, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df6 = SumMerged_df5.merge(paycode_sum6, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df7 = SumMerged_df6.merge(paycode_sum7, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df8 = SumMerged_df7.merge(paycode_sum8, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df9 = SumMerged_df8.merge(paycode_sum9, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df10 = SumMerged_df9.merge(paycode_sum10, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df11 = SumMerged_df10.merge(paycode_sum11, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df12 = SumMerged_df11.merge(paycode_sum12, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df13 = SumMerged_df12.merge(paycode_sum13, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df14 = SumMerged_df13.merge(paycode_sum14, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df15 = SumMerged_df14.merge(paycode_sum15, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df16 = SumMerged_df15.merge(paycode_sum16, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df17 = SumMerged_df16.merge(paycode_sum17, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df18 = SumMerged_df17.merge(paycode_sum18, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df19 = SumMerged_df18.merge(paycode_sum19, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df20 = SumMerged_df19.merge(paycode_sum20, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df21 = SumMerged_df20.merge(paycode_sum21, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df22 = SumMerged_df21.merge(paycode_sum22, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')
SumMerged_df23 = SumMerged_df22.merge(paycode_sum23, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')




SumMerged_df = SumMerged_df23.merge(paycode_sum24, on=['EMPLID', 'concatenated_GradeStepTerm'], how='left')









# Display the result
SumMerged_df.to_csv('summedUnitsClaimed.csv')



# Save the amended DataFrame to a new CSV file
output_file = 'amended_dataset.csv'
sorted_Ndf.to_csv(output_file, index=False)


mergedDF = SumMerged_df.merge(StudentsCourse, right_on= ['Staff_ID', 'Course_ID'] , left_on=['EMPLID', 'concatenated_GradeStepTerm'], how='right')





mergedDF_basedOnWeights = SumMerged_df.merge(TotalWeightbyCourseStaff, right_on= ['Staff_ID', 'Course_ID'] , left_on=['EMPLID', 'concatenated_GradeStepTerm'], how='right')


mergedDF_basedOnWeightsV2 = SumMerged_df.merge(MergedStudentCountandWeight, right_on= ['Staff_ID', 'Course_ID'] , left_on=['EMPLID', 'concatenated_GradeStepTerm'], how='right')




#Find the unmatched staff hours between the two dataframes 
UnMatched_StaffHours = mergedDF[
    mergedDF['Total_Units_Claimed'].isna() | 
    (mergedDF['Total_Units_Claimed'] == '') |
    (mergedDF['Total_Units_Claimed'] == ' ')]


# Specify the output file path
unmatched_staffHrs = 'UnMatched_StaffHoursAuto.csv'

# Write the result to a CSV file
UnMatched_StaffHours.to_csv(unmatched_staffHrs, index=False)


#Return only matched staff hours between the two data sets

Matched_StaffHours = mergedDF[
    mergedDF['Total_Units_Claimed'].notna() &
    (mergedDF['Total_Units_Claimed'] != '') &
    (mergedDF['Total_Units_Claimed'] != ' ')]


# Specify the output file path
matched_staffHrs = 'Matched_StaffHoursAuto.csv'

# Write the result to a CSV file
Matched_StaffHours.to_csv(matched_staffHrs, index=False)


Matched_StaffHour_basedOnWeights =  mergedDF_basedOnWeights[
    mergedDF_basedOnWeights['Total_Units_Claimed'].notna() &
    (mergedDF_basedOnWeights['Total_Units_Claimed'] != '') &
    (mergedDF_basedOnWeights['Total_Units_Claimed'] != ' ')]


Matched_StaffHour_basedOnWeightsV2 =  mergedDF_basedOnWeightsV2[
    mergedDF_basedOnWeightsV2['Total_Units_Claimed'].notna() &
    (mergedDF_basedOnWeightsV2['Total_Units_Claimed'] != '') &
    (mergedDF_basedOnWeightsV2['Total_Units_Claimed'] != ' ')]




'''
# Check if Marking_Units_Claimed exists after the merge
if 'Marking_Units_Claimed' in mergedDF.columns:
    print("Marking_Units_Claimed exists in mergedDF.")
else:
    print("Marking_Units_Claimed does not exist in mergedDF.")
'''

mergedDF.to_csv('mergedData.csv')
print(mergedDF.head())


Matched_StaffHour_basedOnWeights.to_csv('mergedData_basedOnWeight.csv')

Matched_StaffHour_basedOnWeightsV2.to_csv('mergedData_WeightandStuCount.csv')



#print df.columns and then copy and paste names of the columns like below use # to comment out the values we don't want
print(Matched_StaffHour_basedOnWeightsV2.columns)

CorrDf = Matched_StaffHour_basedOnWeightsV2[['EMPLID', 'concatenated_GradeStepTerm', 'Total_Units_Claimed',
       'Marking_Units_Claimed', 'MARKING2_Units_Claimed',
       'MARKSUPVR_Units_Claimed', 
       #'OTHERACT2_Units_Claimed', 'UNITS_CLAIMED_x',
       #'UNITS_CLAIMED_y', 'TUTORING_Units_Claimed', 'LECTBASIC_Units_Claimed',
       #'TRCASUAL_Units_Claimed', 'TUTORRPT2_Units_Claimed',
       #'TUTORRPT_Units_Claimed', 'TUTORSTP2_Units_Claimed',
       #'LECTRPT_Units_Claimed', 'MISCDUTIES_Units_Claimed',
       #'LECTSPEC_Units_Claimed', 'MISCDUT2_Units_Claimed',
       #'LECTDEVEL_Units_Claimed', 'CLINSUN_Units_Claimed', 'OT_Units_Claimed',
       #'SATCASUAL_Units_Claimed', 'CLINSAT_Units_Claimed',
       #'SUNCASUAL_Units_Claimed', 'ADDIT_Units_Claimed',
       #'SESSDAY_Units_Claimed', 'Course_ID', 'Course_Name', 'Staff_ID',
       'Weight_of_Column', 'Unique_Student_Count']].copy()



#correlationMatrix = Matched_StaffHour_basedOnWeightsV2.corr()
correlationMatrix = CorrDf.select_dtypes(include='number').corr()
print(correlationMatrix)

correlationMatrix.to_csv("CorrelationMatrix_basedOnWeightV2.csv")
#print(Matched_StaffHour_basedOnWeightsV2.dtypes)


#plt.figure(figsize=(10,10))
#display correlation in a heatmap

# Rotate x-axis labels

'''
sns.xticks(rotation=45, ha='right')

sns.heatmap(correlationMatrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)


plt.show()

'''

from rapidfuzz import fuzz




plt.figure(figsize=(5, 5)) 
ax = sns.heatmap(correlationMatrix, annot=True, cmap='coolwarm')

# Rotate x-axis labels
ax.set_xticklabels(ax.get_xticklabels(), rotation=360, ha='left', fontsize=8)  # Rotating by 45 degrees

#y-axis labels and adjust font size
ax.set_yticklabels(ax.get_yticklabels(), fontsize=8)

# Show the plot
plt.show()


CAS_StaffMatch = pd.read_csv(r'USC Marking Analysis/summary_hr_data_casual (1).csv')






# Ensure 'EMPLID' and 'Staff_ID' are strings
CAS_StaffMatch['EMPLID'] = CAS_StaffMatch['EMPLID'].astype(str)


StudentsCourse['Staff_ID'] = StudentsCourse['Staff_ID'].astype(str)

#pd.to_numeric(StudentsCourse['Staff_ID'], errors='coerce')

# Rename 'Staff_ID' to 'EMPLID' in StudentsCourse for consistent merging
StudentsCourse.rename(columns={'Staff_ID': 'EMPLID'}, inplace=True)


CAS_StaffMatchUnique = CAS_StaffMatch['EMPLID'].nunique()
print('CAS_StaffMatchUnique amount: ')
print(CAS_StaffMatchUnique)

CAS_StaffMatchUniqueVal = CAS_StaffMatch['EMPLID'].unique()
print('CAS_StaffMatch Unique Values: ')
print(CAS_StaffMatchUniqueVal)


StudentsCourse_Unique = StudentsCourse['EMPLID'].nunique()
print('Student Course Unique amount: ')
print(StudentsCourse_Unique)


StudentsCourse_UniqueVal = StudentsCourse['EMPLID'].unique()

print('Student Course Unique Values: ')
print(StudentsCourse_UniqueVal)


# Convert numpy arrays to Python sets
StudentsCourse_UniqueVal_set = set(StudentsCourse_UniqueVal)
CAS_StaffMatchUniqueVal_set = set(CAS_StaffMatchUniqueVal)

# Find the intersection
matching_values = StudentsCourse_UniqueVal_set.intersection(CAS_StaffMatchUniqueVal_set)

# Count the number of matches
num_matching = len(matching_values)

print(f"Number of matching values: {num_matching}")
print(f"Matching values: {matching_values}")

matching_df =pd.DataFrame(list(matching_values), columns=['MatchingValues'])

matching_df.to_csv('MatchingFromHRDataSet.csv', index=False)

print("Matching values have been saved to 'MatchingFromHRDataSet.csv'.")

# Find the non-matching values (values in CAS_StaffMatchUniqueVal but not in StudentsCourse_UniqueVal)
non_matching_values = CAS_StaffMatchUniqueVal_set.difference(StudentsCourse_UniqueVal_set)

# Count the number of non-matches
num_non_matching = len(non_matching_values)

print(f"Number of non-matching values: {num_non_matching}")
print(f"Non-matching values: {non_matching_values}")


# Convert non-matching values (set) to a DataFrame
non_matching_df = pd.DataFrame(list(non_matching_values), columns=['NonMatchingValues'])



# Find the non-matching values (values in StudentsCourse but not in CAS_StaffMatch)
non_matching_students = StudentsCourse_UniqueVal_set.difference(CAS_StaffMatchUniqueVal_set)

# Count the number of non-matches
num_non_matching_students = len(non_matching_students)

print(f"Number of non-matching values in StudentsCourse: {num_non_matching_students}")
print(f"Non-matching values in StudentsCourse: {non_matching_students}")

# Convert non-matching values (set) to a DataFrame
non_matching_students_df = pd.DataFrame(list(non_matching_students), columns=['NonMatchingValues'])

# Save non-matching values to a CSV file
non_matching_students_df.to_csv('NonMatchingFromStudentCourseDataSet.csv', index=False)

print("Non-matching values have been saved to 'NonMatchingFromStudentCourseDataSet.csv'.")




# Save the DataFrame to a CSV file
non_matching_df.to_csv('nonMatchingFromHRDataSet.csv', index=False)

print("Non-matching values have been saved to 'nonMatchingFromHRDataSet.csv'.")




# Find non-matching values in StudentsCourse
non_matching_from_students = StudentsCourse_UniqueVal_set.difference(CAS_StaffMatchUniqueVal_set)

# Convert the non-matching values to a DataFrame
non_matching_students_df = pd.DataFrame(list(non_matching_from_students), columns=['NonMatchingValues'])

# Save the DataFrame to a CSV file
non_matching_students_df.to_csv('nonMatchingFromStudentsCourse.csv', index=False)

print("Non-matching values from StudentsCourse have been saved to 'nonMatchingFromStudentsCourse.csv'.")


# Merge the DataFrames based on 'EMPLID'
mergedDF_CAS_StaffMatch = pd.merge(StudentsCourse, CAS_StaffMatch, on='EMPLID', how='right')

# Display the head of the merged DataFrame
print(mergedDF_CAS_StaffMatch.head())

mergedDF_CAS_StaffMatch.to_csv('CAS_StaffMatchingBB.csv')


# Identify rows in StudentsCourse that did not match with CAS_StaffMatch
unmatched_staffHrsBB = StudentsCourse[~StudentsCourse['EMPLID'].isin(CAS_StaffMatch['EMPLID'])]

print('Unmatched_staffHrBB results:')
print(unmatched_staffHrsBB.head())

unmatched_staffHrsBB.to_csv('unmatched_staffHrsBB.csv')

#unmatched_emplids = unique_emplids[~unique_emplids['EMPLID'].isin(CAS_StaffMatch['EMPLID'])]
#print(unmatched_emplids)

