from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
import re

########################################################################################
# CONFIG
########################################################################################

manual_snapshot_date = "2026-04-24"

# -------------------------------
# DATABASE CONNECTION (SANITISED)
# -------------------------------
DB_SERVER = "your-sql-server-host"
DB_NAME = "your-database-name"
DB_DRIVER = "ODBC Driver 17 for SQL Server"

conn_str = (
    f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}"
    f"?driver={DB_DRIVER.replace(' ', '+')}"
    f"&trusted_connection=yes"
)

engine = create_engine(conn_str)

# -------------------------------
# PROJECT STRUCTURE (HYPOTHETICAL)
# -------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "..", "data")
INPUT_DIR = os.path.join(DATA_DIR, "input")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")

INPUT_FILE = os.path.join(INPUT_DIR, "var_calculation_input.xlsx")
SAVE_PATH = OUTPUT_DIR

########################################################################################
# CLEAN SHEET NAME
########################################################################################

def clean_sheet_name(name: str) -> str:
    name = re.sub(r'[\\/*?:[\]]', '_', name)
    return name[:31]

########################################################################################
# LOAD INPUT DATA
########################################################################################

Vola = pd.read_excel(INPUT_FILE, sheet_name="Vola")
Korrel = pd.read_excel(INPUT_FILE, sheet_name="Korrel")

########################################################################################
# CLEAN CORRELATION MATRIX
########################################################################################

Korrel = Korrel.rename(columns={Korrel.columns[0]: "Factor_1"})
Korrel_long = Korrel.melt(
    id_vars="Factor_1",
    var_name="Factor_2",
    value_name="Correlation"
)

Korrel_long["Factor_1"] = Korrel_long["Factor_1"].str.replace(r'^\d+\s*-\s*', '', regex=True)
Korrel_long["Factor_2"] = Korrel_long["Factor_2"].str.replace(r'^\d+\s*-\s*', '', regex=True)

Korrel_matrix = Korrel_long.pivot(
    index="Factor_1",
    columns="Factor_2",
    values="Correlation"
).reset_index()

########################################################################################
# CLEAN VOLATILITY MATRIX
########################################################################################

Vola = Vola.rename(columns={Vola.columns[0]: "Factor_1"})
Vola_long = Vola.melt(
    id_vars="Factor_1",
    var_name="Factor_2",
    value_name="Volatility"
)

Vola_long["Factor_1"] = Vola_long["Factor_1"].str.replace(r'^\d+\s*-\s*', '', regex=True)
Vola_long["Factor_2"] = Vola_long["Factor_2"].str.replace(r'^\d+\s*-\s*', '', regex=True)

Vola_matrix = Vola_long.pivot(
    index="Factor_1",
    columns="Factor_2",
    values="Volatility"
).reset_index()

########################################################################################
# LOAD / SAVE TO DATABASE
########################################################################################

Vola_matrix.to_sql(
    "var_volatility_table",
    schema="risk",
    con=engine,
    if_exists="replace",
    index=False
)

Korrel_matrix.to_sql(
    "var_correlation_table",
    schema="risk",
    con=engine,
    if_exists="replace",
    index=False
)

print("✅ Volatility & Correlation tables updated")

########################################################################################
# LOAD PORTFOLIO DATA
########################################################################################

Combined = pd.read_sql(
    "SELECT * FROM risk.var_table",
    engine
)

Combined["new_location"] = Combined["new_location"].astype(str).str.strip()

########################################################################################
# LOAD MATRICES FROM DB
########################################################################################

df_vol = pd.read_sql("SELECT * FROM risk.var_volatility_table", engine)
df_corr = pd.read_sql("SELECT * FROM risk.var_correlation_table", engine)

key_list = list(df_vol.columns[1:])
V_full = df_vol.iloc[:, 1:].to_numpy()
K_full = df_corr.iloc[:, 1:].to_numpy()

########################################################################################
# MAIN VaR FUNCTION
########################################################################################

def run_var_all_locations(Combined, key_list, V_full, K_full, engine, snapshot_date, save_path):

    results = []
    locations = Combined["new_location"].dropna().unique()

    corr_file = os.path.join(save_path, f"corr_{snapshot_date}.xlsx")
    vola_file = os.path.join(save_path, f"vol_{snapshot_date}.xlsx")
    autopv_file = os.path.join(save_path, f"autopv_{snapshot_date}.xlsx")

    for f in [corr_file, vola_file, autopv_file]:
        if not os.path.exists(f):
            pd.DataFrame().to_excel(f, index=False)

    with pd.ExcelWriter(corr_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as corr_writer, \
         pd.ExcelWriter(vola_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as vola_writer, \
         pd.ExcelWriter(autopv_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as autopv_writer:

        for loc in locations:

            df_loc = Combined[Combined["new_location"] == loc].copy()

            titles = df_loc["title"].dropna().str.strip().unique()
            buckets = [30, 60, 90, 150, 200, 280]

            autopv = pd.MultiIndex.from_product(
                [titles, buckets],
                names=["title", "sample_size"]
            ).to_frame(index=False)

            agg = df_loc.groupby(["title", "sample_size"], as_index=False)["total_pv"].sum()
            autopv = autopv.merge(agg, how="left", on=["title", "sample_size"])

            autopv["value"] = autopv["total_pv"].fillna(0)
            autopv["tv"] = autopv["value"].abs()

            total_val = autopv["value"].sum()
            autopv["percentage"] = (autopv["value"] / total_val * 100) if total_val != 0 else 0

            autopv["location"] = loc
            autopv["unique_key"] = (
                autopv["title"].astype(str)
                + "_"
                + autopv["sample_size"].astype(str)
            )
            autopv["snapshot_ts"] = snapshot_date

            sheet_name = clean_sheet_name(loc)

            autopv.to_excel(autopv_writer, sheet_name=sheet_name, index=False)

            matched = autopv[autopv["unique_key"].isin(key_list)].copy()

            if matched.empty:
                continue

            idx_map = {k: i for i, k in enumerate(key_list)}
            idx = [idx_map[k] for k in matched["unique_key"] if k in idx_map]

            V_sub = V_full[np.ix_(idx, idx)]
            K_sub = K_full[np.ix_(idx, idx)]

            names = matched["unique_key"].tolist()

            pd.DataFrame(V_sub, columns=names, index=names).reset_index().to_excel(
                vola_writer, sheet_name=sheet_name, index=False
            )

            pd.DataFrame(K_sub, columns=names, index=names).reset_index().to_excel(
                corr_writer, sheet_name=sheet_name, index=False
            )

            P = matched[["value"]].to_numpy(dtype=float)
            P = np.nan_to_num(P)

            try:
                V_sub = np.nan_to_num(V_sub)
                K_sub = np.nan_to_num(K_sub)

                V_sub = 0.5 * (V_sub + V_sub.T)
                K_sub = 0.5 * (K_sub + K_sub.T)

                eigvals, eigvecs = np.linalg.eigh(V_sub @ K_sub @ V_sub)
                eigvals = np.clip(eigvals, 1e-12, None)

                M = eigvecs @ np.diag(eigvals) @ eigvecs.T

                portfolio_var = float(P.T @ M @ P)

                var_1d = 1.645 * np.sqrt(max(portfolio_var, 0))
                var_5d = var_1d * np.sqrt(5)

            except Exception:
                var_1d, var_5d = np.nan, np.nan

            results.append({
                "snapshot_ts": snapshot_date,
                "location": loc,
                "net_pv": round(matched["value"].sum(), 3),
                "tv": round(matched["tv"].sum(), 3),
                "var_1d": round(var_1d, 3) if pd.notna(var_1d) else None,
                "var_5d": round(var_5d, 3) if pd.notna(var_5d) else None
            })

    return pd.DataFrame(results)

########################################################################################
# RUN
########################################################################################

results_df = run_var_all_locations(
    Combined,
    key_list,
    V_full,
    K_full,
    engine,
    manual_snapshot_date,
    SAVE_PATH
)

########################################################################################
# EXPORT
########################################################################################

results_df.to_sql(
    "var_summary_all_locations",
    schema="risk",
    con=engine,
    if_exists="append",
    index=False
)

print("✅ VaR pipeline completed successfully")
