import os
from os import listdir
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('ggplot')
import math
import statistics
from thefuzz import fuzz, process
import spacy


nlp = spacy.load("en_core_web_lg")


#Merge Dataframes with pandas


files = [file for file in os.listdir(r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV')]

blackboard_data = pd.DataFrame()


for file in files:
    
   grades_df = pd.read_csv(os.path.join(r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV', file), encoding='latin1', low_memory=False)
   blackboard_data = pd.concat([blackboard_data, grades_df], ignore_index=True)  # Use ignore_index=True to reset index


IRM_data = pd.read_csv(r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\IRM Data\IRM_AssessmentComponent_20240926.csv", encoding='ISO-8859-1')





blackboard_data.to_csv('BB_df.csv')
IRM_data2 = IRM_data.copy()

IRM_data2['concatenated_GradeStepTerm'] = (
    IRM_data2['CourseCode'] + '-' + IRM_data2['SISTermId'].astype(str)
)


print(IRM_data2.columns)

print("Number of missing values by column: ")
print(IRM_data2.isnull().sum())


#2. Check for duplicates 

print("Number of duplicated rows: ")
print(IRM_data2.duplicated().sum())


IRM_data3 = IRM_data2[[
   #'SISTermId', 
   'TermName', 
   #'CourseCode', 
   'CourseName', 
   #'CourseState',
       'CoordinatorSISId', 'CoordinatorName', 'AssessmentComponentId',
       #'CourseId', 
       'ComponentNumber', 'AssessmentComponentName', 'WeekDue',
       'Weight', 'IsHurdleTask', 'concatenated_GradeStepTerm']].copy()

print(IRM_data3.head())

print(IRM_data3.columns)




#Keep only rows with weight values for each course ID
'''

IRM_data3[
    IRM_data3['Weight'].notna() & 
    (IRM_data3['Weight'] != '') & 
    (IRM_data3['Weight'] != ' ')
]

'''



blackboard_data = blackboard_data.rename(columns={'Course ID' : 'Course_ID', 'Course Name' : 'Course_Name', 'Grade Centre Title' : 'Grade_Centre_Title',
       'Staff ID' : 'Staff_ID', 'Marker Username' : 'Marker_Username', 'Marker Firstname' : 'Marker_Username',
       'Marker Lastname' : 'Marker_Lastname', 'Latest Attempt' : 'Latest_Attempt', 'Grade Centre Identifier' : 'Grade_Centre_ID',
       'Part of Total Interim (Yes/No) ' : 'partOfTotalInterim', 'Weight of Column' : 'Weight_of_Column' })


print(blackboard_data.columns)

blackboard_data['Staff_ID'] = blackboard_data['Staff_ID'].apply(str)
blackboard_data['Course_ID'] = blackboard_data['Course_ID'].apply(str)
blackboard_data['Latest_Attempt'] = pd.to_datetime(blackboard_data['Latest_Attempt'],   format='%d/%m/%Y', 
                                                    errors='coerce')


blackboard_data.to_csv('BBData.csv')



df = blackboard_data


# Ensure the column is treated as text (string)
df['Grade_Centre_Title'] = df['Grade_Centre_Title'].astype(str)


# Filter rows where the 'column_name' contains any number (digit)
df_filtered = df[df['Grade_Centre_Title'].str.contains(r'\d{4}', na=False)]

# Save the filtered data to a new Excel file
df_filtered.to_csv('filtered_data.csv', index=False)




#Commented out as part of testing - 19.11.24




"""
#Keep only rows with weight values for each course ID
blackboard_data[
    blackboard_data['Weight_of_Column'].notna() & 
    (blackboard_data['Weight_of_Column'] != '') & 
    (blackboard_data['Weight_of_Column'] != ' ')
]
"""



'''
# Extract unique values from both columns
unique_staff_ids = set(blackboard_data['Course_ID'].unique())
unique_coordinator_ids = set(IRM_data3['concatenated_GradeStepTerm'].unique())

# Find the intersection of both sets
matching_ids = unique_staff_ids.intersection(unique_coordinator_ids)

# Display the matching IDs
print("Matching unique values between concatenated_GradeStepTerm values:", len(matching_ids))

'''

# Ensure both columns are strings to prevent data type mismatches
blackboard_data['Course_ID'] = blackboard_data['Course_ID'].astype(str)
IRM_data3['concatenated_GradeStepTerm'] = IRM_data3['concatenated_GradeStepTerm'].astype(str)


#messed up here it should be on the IRM data set that I do this 

#Will to have take a different approach here as the IRM data does not have a datetime column,  Will have to use a spilt function an the TermName column instead


IRM_data3['TermName'] = IRM_data3['TermName'].str.slice(0, 4)



start_year = '2016'

end_year = '2022'



# Filter the DataFrame based on the date range
IRM_data3 = IRM_data3[IRM_data3['TermName'].between(start_year, end_year)]

IRM_data3.to_csv('IRMData3.csv')

# Assuming IRM_data3V1 is already created with 'concatenated_GradeStepTerm', 'AssessmentComponentName', 'Weight'
IRM_data3V1 = IRM_data3[['concatenated_GradeStepTerm', 'AssessmentComponentName', 'Weight']]





IRMmean_weights_df = IRM_data3.groupby(['concatenated_GradeStepTerm', 'AssessmentComponentName'], as_index=False)['Weight'].mean()


# Filter out rows where 'Weight' is equal to 0
IRMmean_weights_df = IRMmean_weights_df[IRMmean_weights_df['Weight'] != 0]

IRMmean_weights_df.to_csv('IRM_meanWeights.csv')


# Normalize the case by converting all 'AssessmentComponentName' values to lowercase (or uppercase)
IRMmean_weights_df['AssessmentComponentName'] = IRMmean_weights_df['AssessmentComponentName'].str.lower()





# Count how frequently each unique 'AssessmentComponentName' occurs in the DataFrame
assessment_component_counts = IRMmean_weights_df['AssessmentComponentName'].value_counts()

# Display the counts of unique 'AssessmentComponentName' values
print('Assessment Counts from IRM:')
print(assessment_component_counts)

# Save the counts to a CSV file
assessment_component_counts.to_csv('AssessmentComponentName_Frequency.csv', header=True)


IRM_data3V1.to_csv('irm_by_course_and_assessments.csv')

#, index=False)



print('IRM Columns')
print(IRM_data3.columns)
print('BB Columns')
print(blackboard_data.columns)



# Select the relevant columns

blackboard_dataV1 = blackboard_data[['Course_ID', 'Grade_Centre_Title', 'Weight_of_Column']]



# Group by 'Course_ID' and 'Grade_Centre_Title', then calculate the mean of 'Weight_of_Column'
mean_weights_df = blackboard_dataV1.groupby(['Course_ID', 'Grade_Centre_Title'], as_index=False)['Weight_of_Column'].mean()

# Display the result to verify
print(mean_weights_df.head())

mean_weights_df.to_csv('BBmeanWeightsDF.csv')


# Normalize the case by converting all 'Grade_Centre_Title' values to lowercase (or uppercase)
mean_weights_df['Grade_Centre_Title'] = mean_weights_df['Grade_Centre_Title'].str.lower()

# Count how frequently each unique 'AssessmentComponentName' occurs in the DataFrame
Grade_CentreTitle_counts = mean_weights_df['Grade_Centre_Title'].value_counts()

# Display the counts of unique 'AssessmentComponentName' values
print('Assessment Counts from IRM:')
print(Grade_CentreTitle_counts)

# Save the counts to a CSV file
Grade_CentreTitle_counts.to_csv('GradeCentreTitle_Frequency.csv', header=True)




blackboard_dataV1.to_csv('BB_byCourseAndAssessment.csv')

# Step 1: Merge the datasets on the matching columns
merged_data = pd.merge(
    blackboard_data,
    IRM_data3,
    left_on='Course_ID',
    right_on='concatenated_GradeStepTerm',
    how='inner'  # Only keep rows where matches exist
)




# Step 2: Select the relevant columns
unique_data = merged_data[['Course_ID', 'Grade_Centre_Title', 'AssessmentComponentName']]


# Step 3: Drop duplicates to ensure uniqueness
unique_data = unique_data.drop_duplicates()

# Step 4 (Optional): Group by Course_ID if needed
grouped_data = unique_data.groupby('Course_ID')


# Optional: Save the unique data to a CSV file
unique_data.to_csv('AssessmentNamesMatch.csv')


# Get unique values from both columns
unique_course_ids = set(blackboard_data['Course_ID'].unique())
unique_grade_step_terms = set(IRM_data3['concatenated_GradeStepTerm'].unique())


# Find matching and non-matching unique values
matching_ids = unique_course_ids.intersection(unique_grade_step_terms)
course_ids_not_in_grade_step_terms = unique_course_ids - unique_grade_step_terms
grade_step_terms_not_in_course_ids = unique_grade_step_terms - unique_course_ids

# Print results
print("Number of matching unique values between Course_ID and concatenated_GradeStepTerm:", len(matching_ids))
print("Number of unique Course_IDs not in concatenated_GradeStepTerm:", len(course_ids_not_in_grade_step_terms))
print("Number of unique concatenated_GradeStepTerm values not in Course_ID:", len(grade_step_terms_not_in_course_ids))






# Group by Course_ID and count unique Grade_Centre_IDs for each course
BB_course_grade_counts = blackboard_data.groupby('Course_ID')['Grade_Centre_ID'].nunique().reset_index()

# Rename columns for clarity
BB_course_grade_counts.columns = ['Course_ID', 'BB_Unique_Grade_Centre_ID_Count']

# Display the result
print(BB_course_grade_counts)


# Group by concatenated_GradeStepTerm and count unique AssessmentComponentId for each term
IRM_term_assessment_counts = IRM_data3.groupby('concatenated_GradeStepTerm')['AssessmentComponentId'].nunique().reset_index()

# Rename columns for clarity
#IRM_term_assessment_counts.columns = ['concatenated_GradeStepTerm', 'IRM-Unique_AssessmentComponentId_Count']

# Display the result
print(IRM_term_assessment_counts.columns)




mergedDF= IRM_term_assessment_counts.merge(BB_course_grade_counts, right_on= ['Course_ID'],  left_on=['concatenated_GradeStepTerm'], how='right')


print(mergedDF.head())

mergedDF.to_csv('ComparisonAssComponetnIDVsUniqueStudent.csv')


