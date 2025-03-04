import subprocess
import time
import runpy
from pathlib import Path


# List of Python files to run in sequence
python_files = [
    # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m4_weekend_and_overtime_penalties_working_file.py",
    # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m4.1_Casual_Academics.py",
    # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m5_ot_span_weekend.py",
    #r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\2- Weekend Penalties and Overtime\m6_ot_daily_weekly.py",   
    #r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m7_meal_allowance_calcs.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me2_timesheet_student.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me3_timesheet_student_perm.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me4_timesheet_me_calc_v2.py",
    #r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me5_Super_UnderPayCalcs.py",
    #r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files - 2nd file\3 - Minimum Engagement\me5b_OT_recalc.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\test_final_underpayment.py"
]



# # Loop through each file and execute it
# for file in python_files:
#     try:
#         print(f"Running {file}...")
#         subprocess.run(["python", file], check=True)
#         print(f"{file} completed successfully.\n")
#     except subprocess.CalledProcessError as e:
#         print(f"Error occurred while running {file}: {e}\n")
#         break


def run_scripts_in_sequence(python_files):
    total_start_time = time.time()
    total_files = len(python_files)
    successful_runs = 0
    
    for index, file in enumerate(python_files, 1):
        file_path = Path(file)
        script_name = file_path.name
        
        print(f"\n[{index}/{total_files}] Running {script_name}...")
        print("-" * 50)
        
        start_time = time.time()
        try:
            # Run the script in the current Python interpreter
            runpy.run_path(file_path, run_name="__main__")
            end_time = time.time()
            duration = end_time - start_time
            successful_runs += 1
            
            print(f"\n✓ {script_name} completed successfully")
            print(f"  Time taken: {duration:.2f} seconds")
            
        except Exception as e:
            print(f"\n✗ Error in {script_name}: {str(e)}")
            print("\nStopping sequence due to dependency failure")
            break
            
        print("-" * 50)
    
    total_duration = time.time() - total_start_time
    print(f"\nExecution Summary:")
    print(f"Total scripts: {total_files}")
    print(f"Successfully completed: {successful_runs}")
    print(f"Total time taken: {total_duration:.2f} seconds")
    print(f"Average time per script: {total_duration/successful_runs:.2f} seconds")

if __name__ == "__main__":
    # Your existing python_files list
    # python_files = [
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\2- Weekend Penalties and Overtime\m4_weekend_and_overtime_penalties_working_file.py",
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\2- Weekend Penalties and Overtime\m4.1_Casual_Academics.py",
    #     # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m5_ot_span_weekend.py",
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\2- Weekend Penalties and Overtime\m5_ot_span_weekend.py", 
    #     # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m6_ot_daily_weekly.py",
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\2- Weekend Penalties and Overtime\m6_ot_daily_weekly.py",
    #     # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m7_meal_allowance_calcs.py",
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\2- Weekend Penalties and Overtime\m7_meal_allowance_calcs.py",
    #     # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me2_timesheet_student.py",
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\me2_timesheet_student.py",
    #    #r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me3_timesheet_student_perm.py",
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\me3_timesheet_student_perm.py",
    #     # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me4_timesheet_me_calc_v2.py",
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\me4_timesheet_me_calc_v2.py",
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\me5_Super_UnderPayCalcs.py",
    #     # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me5b_OT_recalc.py",
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\me5b_OT_recalc.py"
    #     # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\test_final_underpayment.py"
    #     r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\test_final_underpayment.py"
    # ]



    python_files = [
    # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m4_weekend_and_overtime_penalties_working_file.py",
    # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m4.1_Casual_Academics.py",
    # r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m5_ot_span_weekend.py",
    r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\2- Weekend Penalties and Overtime\m6_ot_daily_weekly.py",
    
    #r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m7_meal_allowance_calcs.py",
    r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\me2_timesheet_student.py",
    r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\me3_timesheet_student_perm.py",
    r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\me4_timesheet_me_calc_v2.py",
    #r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me5_Super_UnderPayCalcs.py",
    #r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files - 2nd file\3 - Minimum Engagement\me5b_OT_recalc.py",
    r"C:\Git\Project_Daylight\UniSC _ CodeOnly\Project files\3 - Minimum Engagement\test_final_underpayment.py"
]

        
    run_scripts_in_sequence(python_files)