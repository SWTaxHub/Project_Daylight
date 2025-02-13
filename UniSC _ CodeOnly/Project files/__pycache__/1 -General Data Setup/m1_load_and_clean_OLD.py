import pandas as pd
import numpy as np


# Step 1: Load the Timesheet data and apply correct data types
def load_and_clean_timesheet(file_path, sheet1_name, sheet2_name, output_directory, output_tests):
    # Load both worksheets
    df1 = pd.read_excel(file_path, sheet_name=sheet1_name)
    df2 = pd.read_excel(file_path, sheet_name=sheet2_name)

    # Automatically rename 'Pay Code' in df2 to 'PIN_NM'
    df2.rename(columns={'Pay Code': 'PIN_NM'}, inplace=True)

    # Remove columns that contain all null or blank values
    df1_cleaned = df1.dropna(axis=1, how='all')
    df2_cleaned = df2.dropna(axis=1, how='all')

    # Combine both dataframes
    combined_df = pd.concat([df1_cleaned, df2_cleaned], ignore_index=True)

    # Correct data types based on your specification
    text_columns = ['eFORM_ID', 'NAME', 'EMPLID', 'EMPL_RCD', 'G3FORM_CONDITION', 'G3FORM_STATUS', 'CAL_PRD_ID',
                    'Pay Date', 'PIN_NM', 'G_START_AM_PM', 'G_FINISH_AM_PM', 'DEPTID', 'Department Name',
                    'GL_Cost_Account', 'JOBCODE', 'FULL_PART_TIME', 'Grade-Step OR Course Code',
                    'POSITION_NBR', 'Position Title', 'REPORTS_TO', 'Day of week', 'Weekend Penalty',
                    '> 22/11/2023 Span of Hours']

    date_columns = ['DATE WORKED', 'Claimed Period Begin Date', 'Claimed Period End Date', 'Pay Date',
                    'Fortnight End', 'Report Run Date']

    datetime_columns = ['BEGINDTTM', 'ENDDTTM']

    decimal_columns = ['UNITS_CLAIMED', 'G_START_HOUR', 'G_START_MINUTE', 'G_FINISH_HOUR',
                       'G_FINISH_MINUTE', 'G_BREAK_MINUTES', 'G_ELAPSED_HOURS_WORKED',
                       'G_ELAPSED_MINUTES_WORKED', 'GP_RATE']

    # Convert data types
    combined_df[text_columns] = combined_df[text_columns].astype(str)
    combined_df[date_columns] = combined_df[date_columns].apply(pd.to_datetime, errors='coerce')
    combined_df[datetime_columns] = combined_df[datetime_columns].apply(pd.to_datetime, errors='coerce')
    combined_df[decimal_columns] = combined_df[decimal_columns].apply(pd.to_numeric, errors='coerce')

    # Replace "nan" strings in date columns with NaT (Not a Time)
    for col in date_columns:
        combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce')

    # Replace "nan" strings in datetime columns with NaT
    for col in datetime_columns:
        combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce')

    # Replace "nan" strings in decimal columns with NaN
    for col in decimal_columns:
        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')

    # Now all "nan" values should be properly replaced with NaT or NaN

    print("Timesheet data types updated successfully.")

    print(f"Number of rows: {len(combined_df)}")

    # Step to filter rows where 'BEGINDTTM' or 'ENDDTTM' are not NaN (not blank)
    non_blank_times_df = combined_df[
        ~combined_df[['BEGINDTTM', 'ENDDTTM']].isna().all(axis=1)
        # Keep rows where at least one of BEGINDTTM or ENDDTTM is not NaN
    ]

    # Step to Identify and Save Duplicates based on the specified fields for rows with non-blank times
    duplicates = non_blank_times_df[
        non_blank_times_df.duplicated(
            subset=['EMPLID', 'EMPL_RCD', 'PIN_NM', 'DATE WORKED', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM'],
            keep=False  # Mark all duplicates, not just the second occurrence
        )
    ]

    # Output duplicate rows to an Excel file
    if not duplicates.empty:
        duplicates_file_path = output_tests + 'duplicate_timesheet_rows_with_nonblank_times.xlsx'
        duplicates.to_excel(duplicates_file_path, index=False)
        print(f"Duplicate rows saved to: {duplicates_file_path}")
    else:
        print("No duplicate rows found where BEGINDTTM or ENDDTTM are non-blank.")

    # Step to Remove Duplicates: Drop duplicates only for rows where BEGINDTTM or ENDDTTM is not NaN
    combined_df_filtered = combined_df[
        ~combined_df[['BEGINDTTM', 'ENDDTTM']].isna().all(axis=1)
        # Only operate on rows where BEGINDTTM or ENDDTTM is not NaN
    ]

    print(f"Number of rows: {len(combined_df_filtered)}")

    combined_df_filtered = combined_df_filtered.drop_duplicates(
        subset=['EMPLID', 'EMPL_RCD', 'PIN_NM', 'DATE WORKED', 'UNITS_CLAIMED', 'BEGINDTTM', 'ENDDTTM']
    )

    


    print(f"Number of rows: {len(combined_df_filtered)}")

    # Combine with rows where both BEGINDTTM and ENDDTTM are NaN (unaffected rows)
    combined_df_final = pd.concat(
        [combined_df[combined_df[['BEGINDTTM', 'ENDDTTM']].isna().all(axis=1)], combined_df_filtered])

    print(f"Number of rows after removing duplicates: {len(combined_df_final)}")

    # Save the cleaned timesheet data in Parquet format
    combined_df_final.to_parquet(output_directory + 'cleaned_combined_timesheet_data.parquet', index=False)

    # Save the top 2000 rows of the timesheet data as an Excel sample
    combined_df_final.head(2000).to_excel(output_directory + 'sample_combined_timesheet_data.xlsx', index=False)

    print("Cleaned timesheet data saved.")

    return combined_df_final  # Optional, you can remove this if no reference is needed


# Step 2: Load the HR master data and apply correct data types
def load_and_clean_hr_data(hr_file_path, hr_sheet_name, output_directory):
    hr_df = pd.read_excel(hr_file_path, sheet_name=hr_sheet_name)

    # Correct data types for HR master data
    text_columns_hr = ['EMPLID', 'EMPL_RCD', 'EMPID_EMPL_RCD', 'DEPTID', 'JOBCODE', 'POSITION_NBR',
                       'ACTION', 'ACTION_REASON', 'REG_TEMP', 'FULL_PART_TIME', 'GP_PAYGROUP',
                       'SAL_ADMIN_PLAN', 'GRADE', 'STEP']

    date_columns_hr = ['EFFECTIVE DATE', 'POSITION ENTRY DATE', 'GRADE ENTRY DATE', 'STEP DATE']

    decimal_columns_hr = ['STD_HOURS', 'FTE', 'ANNL_BENEF_BASE_RT', 'HOURLY_RT', 'CHANGE_PCT', 'CHANGE_AMT']

    # Convert data types
    hr_df[text_columns_hr] = hr_df[text_columns_hr].astype(str)
    hr_df[date_columns_hr] = hr_df[date_columns_hr].apply(pd.to_datetime, errors='coerce')
    hr_df[decimal_columns_hr] = hr_df[decimal_columns_hr].apply(pd.to_numeric, errors='coerce')

    print("HR master data types updated successfully.")

    # Save the cleaned HR master data in Parquet format
    hr_df.to_parquet(output_directory + 'cleaned_hr_master_data.parquet', index=False)

    # Save the top 2000 rows of the HR master data as an Excel sample
    hr_df.head(2000).to_excel(output_directory + 'sample_hr_master_data.xlsx', index=False)

    print("Cleaned HR master data saved.")

    return hr_df  # Optional, you can remove this if no reference is needed


# File path and sheet names for timesheet data
#Commented out Paul's file path
#file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\UniSC Data Transfer 2 Sept\Timesheet Data - Master File 2016 -2019 & 2020 - 2024.xlsx'
file_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\UniSC Data Transfer 2 Sept\Timesheet Data - Master File 2016 -2019 & 2020 - 2024.xlsx"
sheet1_name = '2016-2019'
sheet2_name = '2020-2024'

# File path for HR master data
#hr_file_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\UniSC Data Transfer 2 Sept\EMD data.xlsx'
hr_file_path = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\UniSC Data Transfer 2 Sept\EMD data.xlsx"
hr_sheet_name = 'EMD DATA'

# Output directory
#Commented out Paul's file path
#output_directory = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\\'
output_directory = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"
output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'

# Load, clean, and save the timesheet data
load_and_clean_timesheet(file_path, sheet1_name, sheet2_name, output_directory, output_tests)

# Load, clean, and save the HR master data
load_and_clean_hr_data(hr_file_path, hr_sheet_name, output_directory)

print("Data cleaning and saving completed successfully.")
