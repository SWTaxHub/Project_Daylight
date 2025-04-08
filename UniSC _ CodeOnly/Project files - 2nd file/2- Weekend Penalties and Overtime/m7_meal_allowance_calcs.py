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

timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_cas_OT_daily_weekly.parquet"

output_cleaned_data = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"

output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"



Penalties_Recalc = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet"

#output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'
output_tests = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\\"
# Step 2: Load the Parquet files
#timesheet_min_top_up_cals = pd.read_parquet(timesheet_min_top_up_cals_path)
Penalties_Recalc = pd.read_parquet(Penalties_Recalc)

# Step 3: Find all unique EMPID_week_id where any of the specified overtime conditions are met in timesheet_cas_OT_daily_weekly


condition_ot = (
    (Penalties_Recalc['discrepancy_amount_excl'] == 0 |
     Penalties_Recalc['discrepancy_amount_excl'].isna()) 
 
)

# Filter for unique EMPID_week_id where overtime conditions are met
empid_week_id_with_overtime = Penalties_Recalc.loc[condition_ot, 'EMPLID_week_id'].unique()



# Load the timesheet data
timesheet_df = pd.read_parquet(timesheet_path)


#Aggregation Steps
# Step 1: Identify the last row for each EMPLID and date_only combination
timesheet_df['is_last_row'] = (
    (timesheet_df['EMPLID'] == timesheet_df['EMPLID'].shift(-1)) & 
    (timesheet_df['date_only'] == timesheet_df['date_only'].shift(-1))
) == False  # Mark True only when the next row no longer matches the combination

# Step 2: Calculate cumulative sums for each combination of EMPLID and date_only
timesheet_df['Cal_OT_Hours_Sum'] = timesheet_df.groupby(['EMPLID', 'date_only'])['cal_OT_hours'].cumsum()
timesheet_df['G_Break_Minutes_Sum'] = timesheet_df.groupby(['EMPLID', 'date_only'])['G_BREAK_MINUTES'].cumsum()

# Step 3: Store the aggregated values only in the last row
timesheet_df['Cal_OT_Hours_Aggregated'] = np.where(
    timesheet_df['is_last_row'],
    timesheet_df['Cal_OT_Hours_Sum'],
    np.nan
)

timesheet_df['G_Break_Minutes_Aggregated'] = np.where(
    timesheet_df['is_last_row'],
    timesheet_df['G_Break_Minutes_Sum'],
    np.nan
)

# Step 4: Clean up unnecessary columns (optional)
timesheet_df.drop(columns=['is_last_row', 'Cal_OT_Hours_Sum', 'G_Break_Minutes_Sum'], inplace=True)



# Spilt out the time for the ENDDTTM column
timesheet_df['ENDDTTM_timeOnly'] = timesheet_df['ENDDTTM'].dt.time



#Nested loop for applying 
# Define the conditions and their corresponding values
conditions_codes = [
    {
        "condition": (
            (~timesheet_df['DOTW'].isin(['1', '2'])) &  # Check if it's Monday to Friday
            (timesheet_df['Cal_OT_Hours_Aggregated'] > 1) &  # Check if overtime hours are 1 or more
            (timesheet_df['ENDDTTM_timeOnly'] > pd.to_datetime('18:00').time())  # After 18:00
        ),
        "true_value": 'Cond1',
        "false_value": 'N1',
    },
    {
        "condition": (
            (~timesheet_df['DOTW'].isin(['1', '2'])) &  # Check if it's Monday to Friday
            (timesheet_df['ENDDTTM_timeOnly'] > pd.to_datetime('19:00').time()) &  # After 19:00
            (timesheet_df['Cal_OT_Hours_Aggregated'] > 1) &  # At least 1 hour overtime
            (timesheet_df['G_Break_Minutes_Aggregated'] >= 30)  # At least 30-minute break
        ),
        "true_value": 'Cond2',
        "false_value": 'N2',
    },
    {
        "condition": (
            ((timesheet_df['DOTW'].isin(['1', '2'])) | (timesheet_df['Holiday'] != 'Not a Holiday')) &  # Saturday/Sunday/Public Holiday
            (timesheet_df['Cal_OT_Hours_Aggregated'] > 4)  # More than 4 hours of overtime
        ),
        "true_value": 'Cond3',
        "false_value": 'N3',
    },
    {
        "condition": (
            ((timesheet_df['DOTW'].isin(['1', '2'])) | (timesheet_df['Holiday'] != 'Not a Holiday')) &  # Saturday/Sunday/Public Holiday
            (timesheet_df['Cal_OT_Hours_Aggregated'] > 5) &  # More than 5 hours of overtime
            (timesheet_df['G_Break_Minutes_Aggregated'] >= 30)  # At least 30-minute break
        ),
        "true_value": 'Cond4',
        "false_value": 'N4',
    },
]


conditions_values = [
    {
        "condition": (
            (~timesheet_df['DOTW'].isin(['1', '2'])) &  # Check if it's Monday to Friday
            (timesheet_df['Cal_OT_Hours_Aggregated'] > 1) &  # Check if overtime hours are 1 or more
            (timesheet_df['ENDDTTM_timeOnly'] > pd.to_datetime('18:00').time())  # After 18:00
            
        ),
        "true_value": 12.50,
        "false_value": 0,
    },
    {
        "condition": (
            (~timesheet_df['DOTW'].isin(['1', '2'])) &  # Check if it's Monday to Friday
            (timesheet_df['ENDDTTM_timeOnly'] > pd.to_datetime('19:00').time()) &  # After 19:00
            (timesheet_df['Cal_OT_Hours_Aggregated'] > 1) &  # At least 1 hour overtime
            (timesheet_df['G_Break_Minutes_Aggregated'] >= 30)  # At least 30-minute break
        ),
        "true_value": 21,
        "false_value": 0,
    },
    {
        "condition": (
            ((timesheet_df['DOTW'].isin(['1', '2'])) | (timesheet_df['Holiday'] != 'Not a Holiday')) &  # Saturday/Sunday/Public Holiday
            (timesheet_df['Cal_OT_Hours_Aggregated'] > 4)  # More than 4 hours of overtime
        ),
        "true_value": 12.50,
        "false_value": 0,
    },
    {
        "condition": (
            ((timesheet_df['DOTW'].isin(['1', '2'])) | (timesheet_df['Holiday'] != 'Not a Holiday')) &  # Saturday/Sunday/Public Holiday
            (timesheet_df['Cal_OT_Hours_Aggregated'] > 5) &  # More than 5 hours of overtime
            (timesheet_df['G_Break_Minutes_Aggregated'] >= 30)  # At least 30-minute break
        ),
        "true_value": 21,
        "false_value": 0,
    },
]



# Initialize the Meal_Allowance column with default values
timesheet_df['Meal_Allowance_Code'] = 'N/a'

# Add a new column to the dataset
timesheet_df['Meal_Allowance'] = 0  # Initialize the column with a default value (e.g., 0)

# Apply conditions iteratively for Meal allowance codes
for cond in conditions_codes:
    timesheet_df['Meal_Allowance_Code'] = np.where(
        cond["condition"],
        cond["true_value"],  # Value if condition is True
        timesheet_df['Meal_Allowance_Code']  # Keep the current value if False
    )
# Add the override condition where wknd_discrepancy_amount_excl is 0, concatenating the existing value
timesheet_df['Meal_Allowance_Code'] = np.where(
    timesheet_df['EMPLID_week_id'].isin(empid_week_id_with_overtime),
    timesheet_df['Meal_Allowance_Code'].astype(str) + ' | Overridden',  # Concatenate existing value with override value
    timesheet_df['Meal_Allowance_Code']  # Keep the existing value if condition is not met
)

# Apply conditions iteratively for Meal Allowance Values
for cond in conditions_values:
    timesheet_df['Meal_Allowance'] = np.where(
        cond["condition"],
        cond["true_value"],  # Value if condition is True
        timesheet_df['Meal_Allowance']  # Keep the current value if False
    )



# Add the override condition where wknd_discrepancy_amount_excl is 0
timesheet_df['Meal_Allowance'] = np.where(
    timesheet_df['EMPLID_week_id'].isin(empid_week_id_with_overtime),
    0,  # Replace with 0 if true
    timesheet_df['Meal_Allowance']  # Keep the existing value
)



# Display the result
print(timesheet_df)
print("Test to see what columns remain after calculations")
print(list(timesheet_df.columns))

#timesheet_df.to_csv('mealAllowance_payments.csv')

#Commented out 26th Dec to test adding meal allowance to output
#timesheet_df.to_parquet(output_cleaned_data + 'timesheet_min_top_up_cals_MealAllowance.parquet', index=False)

timesheet_df.to_parquet(output_cleaned_data + 'timesheet_cas_OT_daily_weekly.parquet', index=False)

timesheet_df.to_csv(output_cleaned_data + 'timesheet_cas_OT_daily_weekly.csv', index=False)

#596 rows  -  Overidden =  $10,395.50 based on wknd_discrepancy_amount_excl is 0 condition

#919 rows -  Still need to be paid = $14,501 