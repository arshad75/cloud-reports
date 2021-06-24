import os
import glob

account_id = "123456123456543"
os.chdir(os.getcwd())
# for file in glob.glob(f"scan_report_{account_id}_*.json"):
for file in glob.glob(f"scan_report_*.json"):
    os.rename(file, f"scan_report_{account_id}.json")
