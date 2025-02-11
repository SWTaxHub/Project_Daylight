import pandas as pd
from datetime import datetime

# Define output directory where the cleaned HR data is stored
#output_directory = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data'
output_directory = r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\Project Daylight\Outputs\Cleaned Data\\"

# Load the cleaned HR data from Parquet (updated from CSV to Parquet)
hr_cleaned_df = pd.read_parquet(output_directory + r'\cleaned_hr_master_data.parquet')

# Step 1: Create a new 'term_date' field where ACTION == 'TER'
hr_cleaned_df['term_date'] = hr_cleaned_df.apply(
    lambda row: row['EFFECTIVE DATE'] if row['ACTION'] == 'TER' else pd.NaT, axis=1
)

# Function to create the summary table, merge SAL_ADMIN_PLAN, FULL_PART_TIME, JOBCODE, and GP_PAYGROUP,
# and adjust earliest_date based on POSITION ENTRY DATE and latest_date based on ACTION and term_date
def create_summary_table(hr_cleaned_df):
    # Group by EMPID_EMPL_RCD and aggregate the relevant columns
    summary_table = hr_cleaned_df.groupby('EMPID_EMPL_RCD').agg(
        merged_plan=('SAL_ADMIN_PLAN', lambda x: '_'.join(sorted(set(x)))),  # Merge the roles into one string
        full_part_time=('FULL_PART_TIME', lambda x: '_'.join(sorted(set(x)))),  # Concatenate FULL_PART_TIME
        job_code=('JOBCODE', lambda x: '_'.join(sorted(set(x)))),  # Concatenate JOBCODE
        pay_group=('GP_PAYGROUP', lambda x: '_'.join(sorted(set(x)))),  # Concatenate GP_PAYGROUP
        earliest_date=('EFFECTIVE DATE', 'min'),
        position_entry_date=('POSITION ENTRY DATE', 'min'),  # Add POSITION ENTRY DATE to the aggregation
        term_date=('term_date', 'max'),  # Pick up the latest term_date (termination date)
        latest_effective_date=('EFFECTIVE DATE', 'max')  # Pick the max effective date
    ).reset_index()

    # Use today's date
    today = pd.Timestamp(datetime.today().date())

    # Set the latest_date: If term_date exists, use it; otherwise, use today's date
    summary_table['latest_date'] = summary_table.apply(
        lambda row: row['term_date'] if pd.notnull(row['term_date']) else today, axis=1
    )

    # Compare POSITION ENTRY DATE with earliest_date and update earliest_date if position_entry_date is earlier
    summary_table['earliest_date'] = summary_table.apply(
        lambda row: min(row['earliest_date'], row['position_entry_date']) if pd.notnull(row['position_entry_date']) else row['earliest_date'],
        axis=1
    )

    # Drop the extra columns no longer needed in the final output
    summary_table.drop(columns=['term_date', 'latest_effective_date', 'position_entry_date'], inplace=True)

    # Check for duplicate EMPID_EMPL_RCD in the summary table
    duplicates = summary_table[summary_table.duplicated(subset=['EMPID_EMPL_RCD'], keep=False)]

    if not duplicates.empty:
        print("\nDuplicates found based on EMPID_EMPL_RCD after summarizing:")
        print(duplicates[['EMPID_EMPL_RCD', 'merged_plan', 'earliest_date', 'latest_date']])
    else:
        print("\nNo duplicates found for EMPID_EMPL_RCD after summarizing.")

    return summary_table


# Main execution
if __name__ == "__main__":
    # Generate the full HR summary table
    hr_summary_table = create_summary_table(hr_cleaned_df)

    # Save the full HR summary table to Parquet
    hr_summary_file = output_directory + r'\summary_hr_data.parquet'
    hr_summary_table.to_parquet(hr_summary_file, index=False)
    print(f"Full HR summary table saved to {hr_summary_file}")

    # Save the filtered sample to Excel
    sample_summary_hr_output = output_directory + r'\summary_hr_data_sample.xlsx'
    hr_summary_table.head(2000).to_excel(sample_summary_hr_output, index=False)
    print(f"Filtered sample saved to {sample_summary_hr_output}")

    # Generate the summary table excluding casual employees (FULL_PART_TIME contains 'D')
    summary_table_permanent = hr_summary_table[~hr_summary_table['full_part_time'].str.contains('D')]

    # Save the permanent employees summary table to Parquet
    permanent_summary_file = output_directory + r'\summary_hr_data_permanent.parquet'
    summary_table_permanent.to_parquet(permanent_summary_file, index=False)
    print(f"Permanent employees summary table saved to {permanent_summary_file}")
