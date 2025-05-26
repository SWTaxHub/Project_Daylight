import pandas as pd
from datetime import datetime

# Define output directory where the cleaned HR data is stored
#output_directory = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data'
output_directory =r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data"

# Load the cleaned HR data from Parquet (updated from CSV to Parquet)
hr_cleaned_df = pd.read_parquet(output_directory + r'\cleaned_hr_master_data.parquet')

# Complete list of EMPLIDs to check against
emplids_list = [

'1065200',
'1082447',
'1084015',
'1086737',
'1095707',
'1110567',
'1111375',
'1111577',
'1115571',
'1117164',
'1117765',
'1121038',
'1124461',
'1126467',
'1132911',
'1134550',
'1138183',
'1140286',
'1150609',
'1150686',
'1155234',
'1157420',
'1159456',
'1161781',
'1164228',
'1166428',
'1167211',
'9001610',
'9006461',
'9006535',
'9009265',
'9009308',
'9009649',
'9010295',
'9011523',
'9011752',
'9011920',
'9012171',
'9012190',
'9012204',
'9012210',
'9012212',
'9012216',
'9012217',
'9012261',
'9012269',
'9012279',
'9012304',
'9012314'
]
# EMPID_EMPL_RCD
def check_emplids(df, emplid_list, label=""):
    present = df[df['EMPLID'].isin(emplid_list)]['EMPLID'].unique()
    missing = set(emplid_list) - set(present)
    
    print(f"\n--- {label} ---")
    print("Found EMPLIDs:", list(present))
    if missing:
        print("Missing EMPLIDs:", list(missing))
    else:
        print("All EMPLIDs found.")


check_emplids(hr_cleaned_df, emplids_list, "HR Dataset")





# Step 1: Create a new 'term_date' field where ACTION == 'TER'
hr_cleaned_df['term_date'] = hr_cleaned_df.apply(
    lambda row: row['EFFECTIVE DATE'] if row['ACTION'] == 'TER' else pd.NaT, axis=1
)

check_emplids(hr_cleaned_df, emplids_list, "HR Dataset with term_date")


hr_cleaned_check = hr_cleaned_df[hr_cleaned_df['EMPLID'].isin(emplids_list)]

#hr_cleaned_check.to_csv(output_directory + r'\hr_cleaned_check.csv', index=False)

# Function to create the summary table, merge SAL_ADMIN_PLAN, FULL_PART_TIME, and adjust earliest_date
def create_summary_table(hr_cleaned_df):
    # Group by EMPID_EMPL_RCD and aggregate relevant columns
    summary_table = hr_cleaned_df.groupby('EMPID_EMPL_RCD').agg(
        EMPLID=('EMPLID', 'first'),  # Include EMPLID in the aggregation
        merged_plan=('SAL_ADMIN_PLAN', lambda x: '_'.join(sorted(set(x)))),  # Merge the roles into one string
        full_part_time=('FULL_PART_TIME', lambda x: '_'.join(sorted(set(x)))),  # Concatenate FULL_PART_TIME
        earliest_date=('EFFECTIVE DATE', 'min'),
        position_entry_date=('POSITION ENTRY DATE', 'min'),  # Add POSITION ENTRY DATE to the aggregation
        term_date=('term_date', 'max'),  # Pick up the latest term_date (termination date)
        latest_effective_date=('EFFECTIVE DATE', 'max')  # Pick the max effective date
    ).reset_index()

    check_emplids(summary_table, emplids_list, "Summary Table Step 1")


    # Use today's date
    today = pd.Timestamp(datetime.today().date())

    # Set the latest_date: If term_date exists, use it; otherwise, use today's date
    summary_table['latest_date'] = summary_table.apply(
        lambda row: row['term_date'] if pd.notnull(row['term_date']) else today, axis=1
    )
    
    check_emplids(summary_table, emplids_list, "Summary Table Step 2")


    # Compare POSITION ENTRY DATE with earliest_date and update earliest_date if position_entry_date is earlier
    summary_table['earliest_date'] = summary_table.apply(
        lambda row: min(row['earliest_date'], row['position_entry_date']) if pd.notnull(row['position_entry_date']) else row['earliest_date'],
        axis=1
    )

    check_emplids(summary_table, emplids_list, "Summary Table Step 3")

    # Drop the extra columns no longer needed in the final output
    summary_table.drop(columns=['term_date', 'latest_effective_date', 'position_entry_date'], inplace=True)

    check_emplids(summary_table, emplids_list, "Summary Table Step 4")

    # Check for duplicate EMPID_EMPL_RCD in the summary table
    duplicates = summary_table[summary_table.duplicated(subset=['EMPID_EMPL_RCD'], keep=False)]

    if not duplicates.empty:
        print("\nDuplicates found based on EMPID_EMPL_RCD after summarizing:")
        print(duplicates[['EMPID_EMPL_RCD', 'EMPLID', 'merged_plan', 'earliest_date', 'latest_date']])
    else:
        print("\nNo duplicates found for EMPID_EMPL_RCD after summarizing.")

    # Filter out rows where FULL_PART_TIME contains 'D' (i.e., casual employees)
    permanent_employees = summary_table[~summary_table['full_part_time'].str.contains('D')]

    check_emplids(permanent_employees, emplids_list, "Summary Table Step 5")
    # Remove duplicates from the permanent employees dataframe
    permanent_employees = permanent_employees.drop_duplicates()

    check_emplids(permanent_employees, emplids_list, "Summary Table Step 6")

    

    # Return the summary table with permanent employees and EMPLID
    return permanent_employees


# Main execution
if __name__ == "__main__":
    # Generate the summary table with permanent employees
    summary_table_permanent = create_summary_table(hr_cleaned_df)

    # Filter rows where SAL_ADMIN_PLAN has string length greater than 3
    long_sal_admin_plan_df = summary_table_permanent[summary_table_permanent['merged_plan'].str.len() > 3]

    if not long_sal_admin_plan_df.empty:
        # Output the filtered rows to Excel
        long_plan_file = output_directory + r'\long_sal_admin_plan.xlsx'
        long_sal_admin_plan_df.to_excel(long_plan_file, index=False)
        print(f"Rows with SAL_ADMIN_PLAN string length greater than 3 saved to {long_plan_file}")
    else:
        print("No SAL_ADMIN_PLAN values with string length greater than 3 found.")

    # Save the summary table to Parquet (main table)
    summary_table_permanent.to_parquet(output_directory + r'\summary_hr_data_permanent.parquet', index=False)


    check_emplids(summary_table_permanent, emplids_list, "HR Dataset Summary Table")


    # Save the top 2000 rows of the summary table to Excel (sample table)
    summary_table_permanent.head(2000).to_excel(output_directory + r'\summary_hr_data_permanent_sample.xlsx', index=False)

    print("Summary table created and saved successfully.")
