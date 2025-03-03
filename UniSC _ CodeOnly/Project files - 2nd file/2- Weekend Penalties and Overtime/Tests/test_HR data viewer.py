import pandas as pd

# Define the path to the Parquet file and the output directory
parquet_file = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\cleaned_hr_master_data.parquet'
output_directory = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'

# Define the EMPID_EMPL_RCD to filter
target_empid_empl_rcd = '106796711'  # Replace '12345' with the EMPID_EMPL_RCD you want to filter

# Load the cleaned HR master data from Parquet
hr_cleaned_df = pd.read_parquet(parquet_file)

# Filter the DataFrame for the specified EMPID_EMPL_RCD
filtered_df = hr_cleaned_df[hr_cleaned_df['EMPID_EMPL_RCD'] == target_empid_empl_rcd]

# Check if there are any transactions for the specified EMPID_EMPL_RCD
if not filtered_df.empty:
    # Define the output path for the Excel file
    output_file = output_directory + f'filtered_transactions_{target_empid_empl_rcd}.xlsx'

    # Save the filtered data to an Excel file
    filtered_df.to_excel(output_file, index=False)

    print(f"Filtered transactions for EMPID_EMPL_RCD {target_empid_empl_rcd} saved to {output_file}")
else:
    print(f"No transactions found for EMPID_EMPL_RCD {target_empid_empl_rcd}.")
