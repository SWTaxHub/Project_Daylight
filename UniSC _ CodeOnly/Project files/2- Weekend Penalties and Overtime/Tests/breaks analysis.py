import pandas as pd

# Define file paths
timesheet_path = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Cleaned Data\timesheet_min_top_up_cals.parquet'
output_tests = r'C:\Users\zhump\Documents\Data Analytics\Project Daylight\Outputs\Tests\\'

# Load the timesheet data
timesheet_df = pd.read_parquet(timesheet_path)

# Define the condition
condition = (timesheet_df['cal_OT_hours'] > 1) & (timesheet_df['G_BREAK_MINUTES'] > 0)

# Filter rows based on the condition
filtered_df = timesheet_df[condition]

# Output the filtered data for verification
output_file = output_tests + 'filtered_timesheet_cal_OT_breaks.xlsx'
filtered_df.to_excel(output_file, index=False)
print(f"Filtered rows saved to: {output_file}")
