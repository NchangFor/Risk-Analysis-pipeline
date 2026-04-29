import os
import shutil
from datetime import datetime

################################################################################
# ARCHIVE CONFIG (GENERIC PATHS)
################################################################################

source_folder_1 = "./data/input/model_runs"
source_folder_2 = "./data/input/report_file.xlsm"
source_folder_3 = "./data/input/archive_workbook.xlsm"
source_folder_4 = "./data/input/clean_data"
source_folder_5 = "./data/input/reference_matrix.xlsx"
source_folder_6 = "./data/output/sql_exports"

################################################################################
# ARCHIVE DESTINATION
################################################################################

date_stamp = datetime.now().strftime("%Y%m%d")

archive_root = os.path.join("./archive", date_stamp)

################################################################################
# CREATE ARCHIVE FOLDER
################################################################################

os.makedirs(archive_root, exist_ok=True)

################################################################################
# COPY FILES / FOLDERS
################################################################################

shutil.copytree(source_folder_1, os.path.join(archive_root, "model_runs"), dirs_exist_ok=True)
shutil.copy(source_folder_2, archive_root)
shutil.copy(source_folder_3, archive_root)
shutil.copytree(source_folder_4, os.path.join(archive_root, "clean_data"), dirs_exist_ok=True)
shutil.copy(source_folder_5, archive_root)
shutil.copytree(source_folder_6, os.path.join(archive_root, "sql_exports"), dirs_exist_ok=True)

print(f"Archive created successfully at {archive_root}")
