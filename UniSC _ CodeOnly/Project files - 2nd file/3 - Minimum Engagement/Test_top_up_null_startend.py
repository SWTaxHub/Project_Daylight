import pandas as pd

# Define file path
#timesheet_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet'


timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet"


# Load the Parquet file into a DataFrame
timesheet_df = pd.read_parquet(timesheet_path)

# Step 1: Identify `EMPLID_date_only` combinations where Start_null or End_null is TRUE
emplid_date_only_with_null = timesheet_df[
    (timesheet_df['Start_null'] == True) | (timesheet_df['End_null'] == True)
]['EMPLID_date_only'].unique()

# Step 2: Get all rows for the identified `EMPLID_date_only` combinations
condition_1 = timesheet_df['EMPLID_date_only'].isin(emplid_date_only_with_null)

# Step 3: Apply the second condition - `ex_1_3hrs_day` is FALSE
condition_2 = timesheet_df['total_hours'] < 3

# Step 4: Apply the third condition - The count for `EMPLID_date_only` is more than one
emplid_date_only_counts = timesheet_df.groupby('EMPLID_date_only').size()
emplid_date_only_more_than_one = emplid_date_only_counts[emplid_date_only_counts > 1].index
condition_3 = timesheet_df['EMPLID_date_only'].isin(emplid_date_only_more_than_one)

# Step 5: Combine all conditions and filter the DataFrame
filtered_df = timesheet_df[condition_1 & condition_2 & condition_3]

# Output a sample of the filtered transactions to Excel
output_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\filtered_transactions_sample.xlsx"
filtered_df.to_excel(output_path, index=False)

print(f"Filtered sample saved to: {output_path}")
