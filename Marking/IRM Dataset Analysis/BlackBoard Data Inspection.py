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


#files = [file for file in os.listdir(r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV')]

#blackboard_data = pd.DataFrame()


#for file in files:
    
#  grades_df = pd.read_csv(os.path.join(r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\Blackboard (Grade Book) files\GradeLogs2015-22\CSV', file), encoding='latin1', low_memory=False)
#   blackboard_data = pd.concat([blackboard_data, grades_df], ignore_index=True)  # Use ignore_index=True to reset index



# File paths
import pandas as pd
import os

import pandas as pd

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

# Initialise an empty DataFrame
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



blackboard_data['Staff ID'] = blackboard_data['Staff ID'].apply(str)
blackboard_data['Course ID'] = blackboard_data['Course ID'].apply(str)
blackboard_data['Latest Attempt'] = pd.to_datetime(blackboard_data['Latest Attempt'],   format='%d/%m/%Y', 
                                                    errors='coerce')


IRM_data = pd.read_csv(r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\USC_dataSet\Samples 21.10.24\IRM Data\IRM_AssessmentComponent_20240926.csv", encoding='ISO-8859-1')

#Pull in file which contains the Employee Ids that match against the HR Dataset
matchingEMPLIDS = pd.read_csv(r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\SW-Tax\MatchingFromHRDataSet.csv")

#Convert to string
matchingEMPLIDS['MatchingValues'] = matchingEMPLIDS['MatchingValues'].astype(str)

#Remove blank spaces from colummn names
blackboard_data = blackboard_data.rename(columns={'Course ID' : 'Course_ID', 'Course Name' : 'Course_Name', 'Grade Centre Title' : 'Grade_Centre_Title',
       'Staff ID' : 'Staff_ID', 'Marker Username' : 'Marker_Username', 'Marker Firstname' : 'Marker_Username',
       'Marker Lastname' : 'Marker_Lastname', 'Latest Attempt' : 'Latest_Attempt', 'Grade Centre Identifier' : 'Grade_Centre_ID',
       'Part of Total Interim (Yes/No) ' : 'partOfTotalInterim', 'Weight of Column' : 'Weight_of_Column' })


print(blackboard_data.columns)

print('Grade Centre Title value counts for bb unaltered: ')
print(blackboard_data['Grade_Centre_Title'].value_counts())

blackboard_data['Staff_ID'] = blackboard_data['Staff_ID'].apply(str)
blackboard_data['Course_ID'] = blackboard_data['Course_ID'].apply(str)
blackboard_data['Latest_Attempt'] = pd.to_datetime(blackboard_data['Latest_Attempt'],   format='%d/%m/%Y', 
                                                    errors='coerce')


# Group by Course_ID and Grade_Centre_Title, calculate the mean of Weight_of_Column
BB_tranche_1 = blackboard_data.groupby(['Course_ID', 'Grade_Centre_Title']).agg({
    'Weight_of_Column': 'mean'  # Calculate mean
}).reset_index()


# Save the result to a CSV file
BB_tranche_1.to_csv('BB_tranche_grouped_by_title.csv', index=False)

print('BB_trunch_grouped value counts: ')

BB_value_counts = blackboard_data['Grade_Centre_Title'].value_counts()

BB_value_counts.to_csv('BB_value_counts.csv')


blackboard_data.to_csv('blackboard_data.csv')

print('IRM data columns:')
print(IRM_data.columns)

IRM_value_counts = IRM_data['AssessmentComponentName'].value_counts()

IRM_value_counts.to_csv('IRM_value_counts.csv')



print('bb values missing dates:')
# Identify rows where 'Latest_Attempt' is blank, NaN, or null
missing_latest_attempt_rows = blackboard_data[
    (blackboard_data['Latest_Attempt'].isna()) |  # Check for NaN
    (blackboard_data['Latest_Attempt'] == '') |  # Check for empty strings
    (blackboard_data['Latest_Attempt'] == ' ')   # Check for single spaces
]


# Output rows with missing 'Latest_Attempt'
print("Rows where 'Latest_Attempt' is blank, NaN, or null:")
print(missing_latest_attempt_rows)






print('Unique Grade Centre Titles in missing latest attempt rows: ')
uniqueGCT = missing_latest_attempt_rows['Grade_Centre_Title'].value_counts()

uniqueGCT.to_csv('uniqueGCT.csv')



missing_latest_attempt_rows.to_csv('missing_latest_attempt_rows.csv')
# Inspect the unique ways missing values appear in the column



# Group by Course_ID and Grade_Centre_Title, calculate the mean of Weight_of_Column
missing_latest_attempt_rows_meanWeights = missing_latest_attempt_rows.groupby(['Course_ID', 'Grade_Centre_Title']).agg({
    'Weight_of_Column': 'mean'  # Calculate mean
}).reset_index()

# Rename the column for clarity
#BB_trunch_grouped(columns={'Weight_of_Column': 'Mean_Weight_of_Column'}, inplace=True)

# Save the result to a CSV file
missing_latest_attempt_rows_meanWeights.to_csv('missing_latest_attempt_grouped_by_title.csv', index=False)



unique_missing_patterns = blackboard_data['Latest_Attempt'].value_counts(dropna=False)
print("\nUnique patterns in 'Latest_Attempt' (including missing values):")
print(unique_missing_patterns)



nan_staffID_rows = blackboard_data[blackboard_data['Staff_ID'] == 'nan'] 



nan_staffID_rows.to_csv('rowsWhereStaffIDisNan.csv')




'''
OGblackboard_LatestAttemptHasVals = blackboard_data[
    blackboard_data['Latest_Attempt'].notna() | 
    (blackboard_data['Latest_Attempt'] != '') |
    (blackboard_data['Latest_Attempt'] != ' ')
]


print('OGblackboard_LatestAttemptHasVals: ', len(OGblackboard_LatestAttemptHasVals))



OGblackboard_LatestAttemptHasNoVals = blackboard_data[
    blackboard_data['Latest_Attempt'].isna() |
    (blackboard_data['Latest_Attempt'] == '') |
    (blackboard_data['Latest_Attempt'] == ' ')
]


print('OGblackboard_LatestAttemptHasNoVals: ', len(OGblackboard_LatestAttemptHasNoVals))

'''

OGblackboard_LatestAttemptHasVals = blackboard_data[
    blackboard_data['Latest_Attempt'].notna() &  # Not null
    (blackboard_data['Latest_Attempt'].astype(str).str.strip() != '')  # Convert to string, strip, and check non-empty
]

print('OGblackboard_LatestAttemptHasVals: ', len(OGblackboard_LatestAttemptHasVals))

OGblackboard_LatestAttemptHasNoVals = blackboard_data[
    blackboard_data['Latest_Attempt'].isna() |  # Null
    (blackboard_data['Latest_Attempt'].astype(str).str.strip() == '')  # Convert to string, strip, and check empty
]

print('OGblackboard_LatestAttemptHasNoVals: ', len(OGblackboard_LatestAttemptHasNoVals))





blackboard_data_condensed = pd.DataFrame()




# Filter CAS_StaffMatch to only include rows where EMPLID is in matching_values
blackboard_data_condensed = blackboard_data[blackboard_data['Staff_ID'].isin(matchingEMPLIDS['MatchingValues'])]

print('Head of blackboard data condensed')
print(blackboard_data_condensed.head)


blackboard_data_condensed.to_csv('blackboard_data_condensed.csv') 

print('blackboard_data_condensed shape:')
print(blackboard_data_condensed.shape)



# Group by Course_ID and Grade_Centre_Title, calculate the mean of Weight_of_Column
condensed_trunch_grouped = blackboard_data_condensed.groupby(['Course_ID', 'Grade_Centre_Title']).agg({
    'Weight_of_Column': 'mean'  # Calculate mean
}).reset_index()

# Rename the column for clarity
condensed_trunch_grouped.rename(columns={'Weight_of_Column': 'Mean_Weight_of_Column'}, inplace=True)

condensed_trunch_grouped.to_csv('condensed_trunch_grouped.csv')



# Identify rows where 'Latest_Attempt' is blank, NaN, or null
missing_latest_attempt_rows_condensed = blackboard_data_condensed[
    (blackboard_data['Latest_Attempt'].isna()) |  # Check for NaN
    (blackboard_data['Latest_Attempt'] == '') |  # Check for empty strings
    (blackboard_data['Latest_Attempt'] == ' ')   # Check for single spaces
]


# Output rows with missing 'Latest_Attempt'
print("Rows where 'Latest_Attempt' is blank, NaN, or null (from Condensed data set):")
print(missing_latest_attempt_rows_condensed)


missing_latest_attempt_rows_condensed.to_csv('missing_latest_attempt_rows_condensed.csv')

blackboard_LatestAttemptHasVals = blackboard_data_condensed[
    blackboard_data['Latest_Attempt'].notna() &  # Not null
    (blackboard_data['Latest_Attempt'].astype(str).str.strip() != '')  # Convert to string, strip, and check non-empty
]

print('blackboard_LatestAttemptHasVals: ', len(blackboard_LatestAttemptHasVals))

blackboard_LatestAttemptHasNoVals = blackboard_data_condensed[
    blackboard_data['Latest_Attempt'].isna() |  # Null
    (blackboard_data['Latest_Attempt'].astype(str).str.strip() == '')  # Convert to string, strip, and check empty
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
blackboard_data['Course_ID'] = blackboard_data['Course_ID'].astype(str)


print('BB Columns')
print(blackboard_data.columns)



# Select the relevant columns

blackboard_dataV1 = blackboard_data_condensed[['Course_ID', 'Grade_Centre_Title', 'Weight_of_Column']]


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
print('Assessment Counts from BB:')
print(len(Grade_CentreTitle_counts))
print(Grade_CentreTitle_counts)

# Save the counts to a CSV file
Grade_CentreTitle_counts.to_csv('GradeCentreTitle_Frequency.csv', header=True)




#df['digits'] = Reduced_Grade_CentreTitle_counts['Grade_Centre_Title'].str.extract(r'(\d+)', expand=False)
#print(df)


#blackboard_dataV1.to_csv('BB_byCourseAndAssessment.csv')





# Group by Course_ID and count unique Grade_Centre_IDs for each course
#BB_course_grade_counts = blackboard_data.groupby('Course_ID')['Grade_Centre_ID'].nunique().reset_index()

BB_course_grade_counts = blackboard_data_condensed.groupby('Course_ID')['Grade_Centre_ID'].nunique().reset_index()

# Rename columns for clarity
BB_course_grade_counts.columns = ['Course_ID', 'BB_Unique_Grade_Centre_ID_Count']

# Display the result
print(len(BB_course_grade_counts))


final_trunch = pd.read_csv(r'C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\SW-Tax\Final_ReductionDataSet.csv')

'''
final_trunch_grouped = final_trunch.groupby('Course_ID')['Grade_Centre_ID'].nunique().reset_index()

final_trunch_grouped.to_csv('final_trunch_grouped.csv')
'''


# Group by Course_ID and Grade_Centre_Title, calculate the mean of Weight_of_Column
final_trunch_grouped = final_trunch.groupby(['Course_ID', 'Grade_Centre_Title']).agg({
    'Weight_of_Column': 'mean'  # Calculate mean
}).reset_index()

# Rename the column for clarity
final_trunch_grouped.rename(columns={'Weight_of_Column': 'Mean_Weight_of_Column'}, inplace=True)

# Save the result to a CSV file
final_trunch_grouped.to_csv('final_trunch_grouped_by_title.csv', index=False)

# Display the resulting DataFrame
print(final_trunch_grouped)


