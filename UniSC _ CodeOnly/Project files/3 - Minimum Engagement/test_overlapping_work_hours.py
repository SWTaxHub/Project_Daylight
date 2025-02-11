import pandas as pd

# Define the file path for the Parquet file
#timesheet_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\cleaned_combined_timesheet_data.parquet'
timesheet_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\timesheet_with_student_and_perm_indicator.parquet"


# Load the Parquet file
timesheet_df = pd.read_parquet(timesheet_path)

# Step 1: Exclude rows where 'BEGINDTTM' or 'ENDDTTM' is null
timesheet_df_clean = timesheet_df.dropna(subset=['BEGINDTTM', 'ENDDTTM'])

# Step 2: Find duplicates based on the selected columns
duplicate_rows = timesheet_df_clean[timesheet_df_clean.duplicated(subset=['EMPLID', 'EMPL_RCD', 'BEGINDTTM', 'ENDDTTM', 'PIN_NM', 'UNITS_CLAIMED'], keep=False)]

# Step 3: Output the duplicate rows to a new file for review
#output_file = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\duplicate_timesheet_rows.xlsx'
output_file = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Tests\duplicate_timesheet_rows.xlsx"


duplicate_rows.to_excel(output_file, index=False)

print(f"Duplicate rows saved to: {output_file}")
