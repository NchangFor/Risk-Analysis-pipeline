# -*- coding: utf-8 -*-
"""
Value at Risk (VaR) Data Pipeline
Portfolio / Demonstration Version

NOTE:
- All file paths and server names are anonymized
- This version is safe for GitHub / public portfolio use
"""

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import os

########################################################################################
# CONFIG
########################################################################################

manual_snapshot_date = "2026-04-24"

# -------------------------------
# PROJECT DIRECTORY STRUCTURE
# -------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "..", "data")
INPUT_DIR = os.path.join(DATA_DIR, "input")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")

# Input files (HYPOTHETICAL)
CLEAN_VAR_PRICE_FILE = os.path.join(INPUT_DIR, "clean_var_price.xlsx")
FUTURES_CONVERSION_FILE = os.path.join(INPUT_DIR, "futures_period_conversion.xlsx")
MATRIX_FILE = os.path.join(INPUT_DIR, "matrix_price_curve_db.xlsx")

# Output files (HYPOTHETICAL)
OUTPUT_VAR_TABLE = os.path.join(OUTPUT_DIR, "var_table_output.xlsx")
OUTPUT_MATRIX = os.path.join(OUTPUT_DIR, "matrix_output.xlsx")
OUTPUT_CLEAN_PRICE = os.path.join(OUTPUT_DIR, "clean_var_price_output.xlsx")

########################################################################################
# CLEAN VAR PRICE SHEET IMPORT
########################################################################################

CleanVaRPrice = pd.read_excel(CLEAN_VAR_PRICE_FILE, sheet_name="Sheet1", engine="openpyxl")

CleanVaRPrice = CleanVaRPrice.rename(columns={
    "TIC": "A",
    "RIC": "B",
    "Close Price": "Price",
    "Expiration Date": "Exp Date"
})

# Remove spaces
CleanVaRPrice["A"] = CleanVaRPrice["A"].astype(str).str.replace(" ", "", regex=False)
CleanVaRPrice["B"] = CleanVaRPrice["B"].astype(str).str.replace(" ", "", regex=False)
CleanVaRPrice["Exp Date"] = CleanVaRPrice["Exp Date"].astype(str).str.strip()

# Remove empty rows
CleanVaRPrice = CleanVaRPrice[
    ~(
        (CleanVaRPrice["A"] == "") &
        (CleanVaRPrice["B"] == "") &
        (CleanVaRPrice["Exp Date"] == "")
    )
]

# Export cleaned version (local / demo output)
CleanVaRPrice.to_excel(OUTPUT_CLEAN_PRICE, index=False)

########################################################################################
# FUTURES CONVERSION SHEET
########################################################################################

Futures_Period_Convertor = pd.read_excel(FUTURES_CONVERSION_FILE, header=0)

########################################################################################
# MATRIX PRICE CURVE DATABASE
########################################################################################

# NOTE: In production this would be a DB connection.
# For portfolio version we assume local file OR environment variable.

# Example placeholder DB config (safe for GitHub)
DB_SERVER = "your-sql-server-host"
DB_NAME = "your-database-name"
DB_DRIVER = "ODBC Driver 17 for SQL Server"

conn_str = (
    f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}"
    f"?driver={DB_DRIVER.replace(' ', '+')}"
    f"&trusted_connection=yes"
)

engine = create_engine(conn_str)

# Load matrix file (hypothetical local dataset for portfolio version)
Matrix = pd.read_excel(MATRIX_FILE, sheet_name="Matrix", header=None)

Matrix.columns = Matrix.iloc[0]
Matrix = Matrix.drop(0).reset_index(drop=True)

########################################################################################
# CLEAN MATRIX
########################################################################################

dtype_dict = {
    "Key": str,
    "Commodity": str,
    "Parity": str,
    "Origin": str,
    "Price curve Flat": object,
    "Price curve for Basis": str,
}

for col, col_type in dtype_dict.items():
    if col in Matrix.columns:
        try:
            Matrix[col] = Matrix[col].astype(col_type)
        except Exception:
            pass

Matrix = Matrix.drop(columns=[c for c in ["Column1", "Column2", "Column4", "Comment"] if c in Matrix.columns])
Matrix = Matrix[Matrix["Key"].astype(str).str.strip() != ""]
Matrix["Commodity"] = Matrix["Commodity"].str.strip().str.title()
Matrix["Parity"] = Matrix["Parity"].str.strip().str.title()
Matrix["Origin"] = Matrix["Origin"].str.strip().str.title()

# Export cleaned matrix
Matrix.to_excel(OUTPUT_MATRIX, index=False)
