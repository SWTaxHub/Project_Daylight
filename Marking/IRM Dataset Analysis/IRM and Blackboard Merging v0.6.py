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

from rapidfuzz import fuzz




nlp = spacy.load("en_core_web_lg")


#Merge Dataframes with pandas


# List of file paths
file_list = [
    r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV\2016 - Grade Centre Log.csv',
    r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV\2017 - Grade Centre Log.csv',
    r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV\2018 - Grade Centre Log.csv',
    r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV\2019 - Grade Centre Log.csv',
    r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV\2020 - Grade Centre Log.csv',
    r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV\2021 - Grade Centre Log.csv',
    r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV\2022 - Grade Centre Log.csv'
]

# Initialize an empty DataFrame
blackboard_data = pd.DataFrame()

# Read each file with latin1 encoding
for file in file_list:
    try:
        print(f"Reading file: {file}")
        temp_df = pd.read_csv(file, encoding='latin1', low_memory=False, na_values=['NULL'])
        blackboard_data = pd.concat([blackboard_data, temp_df], ignore_index=True)
    except UnicodeDecodeError as e:
        print(f"Error reading file {file}: {e}")

# Output the final DataFrame shape
print("Data concatenation complete. Shape:", blackboard_data.shape)

IRM_data = pd.read_csv(r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\IRM Data\IRM_AssessmentComponent_20240926.csv", encoding='ISO-8859-1')

matchingEMPLIDS = pd.read_csv(r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\SW-Tax\MatchingFromHRDataSet.csv")


matchingEMPLIDS['MatchingValues'] = matchingEMPLIDS['MatchingValues'].astype(str)









#blackboard_data.to_csv('BB_df.csv')
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


blackboard_data.to_csv('blackboard_data.csv')


nan_staffID_rows = blackboard_data[blackboard_data['Staff_ID'] == 'nan'] 



nan_staffID_rows.to_csv('rowsWhereStaffIDisNan.csv')




blackboard_data_condensed = pd.DataFrame()




# Filter CAS_StaffMatch to only include rows where EMPLID is in matching_values
blackboard_data_condensed = blackboard_data[blackboard_data['Staff_ID'].isin(matchingEMPLIDS['MatchingValues'])]

print('Head of blackboard data condensed')
print(blackboard_data_condensed.head)


blackboard_data_condensed.to_csv('blackboard_data_condensed.csv') 

print('blackboard_data_condensed shape:')
print(blackboard_data_condensed.shape)




blackboard_LatestAttemptHasVals = blackboard_data_condensed[
    blackboard_data_condensed['Latest_Attempt'].notna() | 
    (blackboard_data_condensed['Latest_Attempt'] != '') |
    (blackboard_data_condensed['Latest_Attempt'] != ' ')
]


print('blackboard_LatestAttemptHasVals: ', len(blackboard_LatestAttemptHasVals))



blackboard_LatestAttemptHasNoVals = blackboard_data_condensed[
    blackboard_data_condensed['Latest_Attempt'].isna() |
    (blackboard_data_condensed['Latest_Attempt'] == '') |
    (blackboard_data_condensed['Latest_Attempt'] == ' ')
]


print('blackboard_LatestAttemptHasNoVals: ', len(blackboard_LatestAttemptHasNoVals))

Unique_assessmentTitles = blackboard_LatestAttemptHasNoVals['Grade_Centre_Title'].value_counts()

# Display the counts of unique 'AssessmentComponentName' values
print('Assessment Counts from BB:')
print(len(Unique_assessmentTitles))
print(Unique_assessmentTitles)

# Save the counts to a CSV file
Unique_assessmentTitles.to_csv('assessmentTitle0+_Frequency.csv', header=True)




#blackboard_data_reduced = blackboard_data[blackboard_data['Latest_Attempt'].isnull()]

# Check the resulting rows\

#print(blackboard_data_reduced.shape)


'''
blackboard_data_reduced = blackboard_data[
   blackboard_data['Latest_Attempt'].isnull() &
   (blackboard_data['Latest_Attempt'] == '') &
   (blackboard_data['Latest_Attempt'] == ' ')]




print('BlackBoard data reduced Shape:')
blackboard_data_reduced.to_csv('BBDataReduced.csv')



df = blackboard_data


# Ensure the column is treated as text (string)
df['Grade_Centre_Title'] = df['Grade_Centre_Title'].astype(str)


# Filter rows where the 'column_name' contains any number (digit)
df_filtered = df[df['Grade_Centre_Title'].str.contains(r'\d{4}', na=False)]

# Save the filtered data to a new Excel file
df_filtered.to_csv('filtered_data.csv', index=False)


#Keep only rows with weight values for each course ID
blackboard_data[
    blackboard_data['Weight_of_Column'].notna() & 
    (blackboard_data['Weight_of_Column'] != '') & 
    (blackboard_data['Weight_of_Column'] != ' ')
]


'''

# Ensure both columns are strings to prevent data type mismatches
blackboard_data_condensed['Course_ID'] = blackboard_data_condensed['Course_ID'].astype(str)
IRM_data3['concatenated_GradeStepTerm'] = IRM_data3['concatenated_GradeStepTerm'].astype(str)


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

blackboard_dataV1 = blackboard_data_condensed[['Course_ID', 'Grade_Centre_Title', 'Weight_of_Column']]


# Group by 'Course_ID' and 'Grade_Centre_Title', then calculate the mean of 'Weight_of_Column'
BB_mean_weights_df = blackboard_dataV1.groupby(['Course_ID', 'Grade_Centre_Title'], as_index=False)['Weight_of_Column'].mean()


# Display the result to verify

print('BB Mean Weights')
print(BB_mean_weights_df.head())
print('IRM Mean Weights')
print(IRMmean_weights_df.head)

BB_mean_weights_df.to_csv('BBmeanWeightsDF.csv')




# Normalize the case by converting all 'Grade_Centre_Title' values to lowercase (or uppercase)
#BB_mean_weights_df['Grade_Centre_Title'] = BB_mean_weights_df['Grade_Centre_Title'].str.lower()


BB_weight_counts = pd.DataFrame()
IRM_weight_counts = pd.DataFrame()

# Rename concatenated_GradeStepTerm in IRMmean_weights_df to Course_ID for consistency
IRMmean_weights_df = IRMmean_weights_df.rename(columns={'concatenated_GradeStepTerm': 'Course_ID'})

# Group by Course_ID and count weights
BB_weight_counts = BB_mean_weights_df.groupby('Course_ID')['Weight_of_Column'].count().reset_index()
IRM_weight_counts = IRMmean_weights_df.groupby('Course_ID')['Weight'].count().reset_index()

# Rename count columns for clarity
BB_weight_counts.rename(columns={'Weight_of_Column': 'BB_Weights_Count'}, inplace=True)
IRM_weight_counts.rename(columns={'Weight': 'IRM_Weights_Count'}, inplace=True)

# Merge on Course_ID
merged_counts = pd.merge(BB_weight_counts, IRM_weight_counts, on='Course_ID', how='outer')

# Fill NaN with 0 and convert to integers
merged_counts['BB_Weights_Count'] = merged_counts['BB_Weights_Count'].fillna(0).astype(int)
merged_counts['IRM_Weights_Count'] = merged_counts['IRM_Weights_Count'].fillna(0).astype(int)

# Identify matching weights
merged_counts['MatchingWeights'] = merged_counts['BB_Weights_Count'] == merged_counts['IRM_Weights_Count']

# Filter for matching courses
matching_courses = merged_counts[merged_counts['MatchingWeights']]


matching_courses.to_csv('matching_courses.csv')

print("Matching weights found for these Course_IDs:")
print(matching_courses[['Course_ID', 'BB_Weights_Count', 'IRM_Weights_Count']])



# Filter the rows where weights don't match
NonMatchingWeights = merged_counts[merged_counts['BB_Weights_Count'] != merged_counts['IRM_Weights_Count']]

# Display the non-matching weights DataFrame
print(NonMatchingWeights)

# Optionally, save to CSV for further inspection
NonMatchingWeights.to_csv('NonMatchingWeights.csv', index=False)

print("Non-Matching weights found for these Course_IDs:")
print(NonMatchingWeights[['Course_ID', 'BB_Weights_Count', 'IRM_Weights_Count']])


BB_weight_counts_all = BB_mean_weights_df.groupby('Course_ID')['Weight_of_Column'].size().reset_index(name='Count')

print('BB_weight_counts_all: ')
print(BB_weight_counts_all)



# Count how frequently each unique 'AssessmentComponentName' occurs in the DataFrame

Grade_CentreTitle_counts = BB_mean_weights_df['Grade_Centre_Title'].value_counts()

# Display the counts of unique 'AssessmentComponentName' values
print('Assessment Counts from BB:')
print(len(Grade_CentreTitle_counts))
print(Grade_CentreTitle_counts)

# Save the counts to a CSV file
Grade_CentreTitle_counts.to_csv('GradeCentreTitle_Frequency.csv', header=True)




#df['digits'] = Reduced_Grade_CentreTitle_counts['Grade_Centre_Title'].str.extract(r'(\d+)', expand=False)
#print(df)


#blackboard_dataV1.to_csv('BB_byCourseAndAssessment.csv')

# Step 1: Merge the datasets on the matching columns
merged_data = pd.merge(
    blackboard_data_condensed,
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
unique_course_ids = set(blackboard_data_condensed['Course_ID'].unique())



print('unique_course_ids in bb condensed: ')
print(len(unique_course_ids))

#unique_course_ids in bb condensed: 7747


unique_staff_IDs = set(blackboard_data_condensed['Staff_ID'].unique())

print('unique_staff_IDs from BB condensed: ')
print(len(unique_staff_IDs))

#unique_staff_IDs from BB condensed: 2352


#Commented out so we can just look at unquie course IDs from the condensed data set

#unique_grade_step_terms = set(IRM_data3['concatenated_GradeStepTerm'].unique())


'''
# Find matching and non-matching unique values
matching_ids = unique_course_ids.intersection(unique_grade_step_terms)
course_ids_not_in_grade_step_terms = unique_course_ids - unique_grade_step_terms
grade_step_terms_not_in_course_ids = unique_grade_step_terms - unique_course_ids

# Print results
print("Number of matching unique values between Course_ID and concatenated_GradeStepTerm:", len(matching_ids))
print("Number of unique Course_IDs not in concatenated_GradeStepTerm:", len(course_ids_not_in_grade_step_terms))
print("Number of unique concatenated_GradeStepTerm values not in Course_ID:", len(grade_step_terms_not_in_course_ids))

'''



# Group by Course_ID and count unique Grade_Centre_IDs for each course
#BB_course_grade_counts = blackboard_data.groupby('Course_ID')['Grade_Centre_ID'].nunique().reset_index()

BB_course_grade_counts = blackboard_data_condensed.groupby('Course_ID')['Grade_Centre_ID'].nunique().reset_index()

# Rename columns for clarity
BB_course_grade_counts.columns = ['Course_ID', 'BB_Unique_Grade_Centre_ID_Count']

# Display the result
print(len(BB_course_grade_counts))


# Group by concatenated_GradeStepTerm and count unique AssessmentComponentId for each term
IRM_term_assessment_counts = IRM_data3.groupby('concatenated_GradeStepTerm')['AssessmentComponentId'].nunique().reset_index()

# Rename columns for clarity
#IRM_term_assessment_counts.columns = ['concatenated_GradeStepTerm', 'IRM-Unique_AssessmentComponentId_Count']

# Display the result
print(IRM_term_assessment_counts.columns)






mergedDF= IRM_term_assessment_counts.merge(BB_course_grade_counts, right_on= ['Course_ID'],  left_on=['concatenated_GradeStepTerm'], how='right')

# Calculate variance between two columns and store it in a new column
mergedDF['Variance'] = (mergedDF['AssessmentComponentId'] - mergedDF['BB_Unique_Grade_Centre_ID_Count'])

print(mergedDF.head())


# Filter out rows where 'Weight' is equal to 0
mergedDF_Variances = mergedDF[mergedDF['Variance'] != 0]



mergedDF.to_csv('ComparisonAssComponetnIDVsUniqueStudent.csv')

mergedDF_Variances.to_csv('NoZeroVarinances.csv')


#Add reduced data set from blackboard based on values being within the 'Latest Attempt' column

'''
# Assuming 'df1' and 'df2' are DataFrames
#mean_weights_df['Best_Match'] = mean_weights_df['Grade_Centre_Title'].apply(
 #   lambda x: max(IRM_data3['AssessmentComponentName'], key=lambda y: fuzz.ratio(x, y))
#)

'''
referenceValues = mergedDF_Variances['Course_ID']

matching_rows = BB_mean_weights_df[BB_mean_weights_df['Course_ID'].isin(referenceValues)]

print(matching_rows.head)

matching_rows.to_csv('matching_rows.csv', index=False)

