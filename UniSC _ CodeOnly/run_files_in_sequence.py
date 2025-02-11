import subprocess

# List of Python files to run in sequence
python_files = [
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m4_weekend_and_overtime_penalties_working_file.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m4.1_Casual_Academics.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m5_ot_span_weekend.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m6_ot_daily_weekly.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\2- Weekend Penalties and Overtime\m7_meal_allowance_calcs.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me2_timesheet_student.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me3_timesheet_student_perm.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me4_timesheet_me_calc_v2.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me5_Super_UnderPayCalcs.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\me5b_OT_recalc.py",
    r"C:\Users\smits\OneDrive - SW Accountants & Advisors Pty Ltd\Desktop\UniSC _ PaulsCode\Project files\3 - Minimum Engagement\test_final_underpayment.py"
]



# Loop through each file and execute it
for file in python_files:
    try:
        print(f"Running {file}...")
        subprocess.run(["python", file], check=True)
        print(f"{file} completed successfully.\n")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {file}: {e}\n")
        break
