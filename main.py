# ICTB_ENTRIES


import os

import duckdb as db
import pandas as pd
from dotenv import load_dotenv
from sqlglot import transpile
import shutil

path = os.path.dirname(os.path.abspath(__file__))

load_dotenv()

file_path = os.environ["smx"]

smx_tables = pd.read_excel(file_path, sheet_name="Stg Tables")
smx_tables = smx_tables.rename(
    columns={
        "Table name": "table_name",
        "Column name": "column_name",
    }
)

scripts = db.query(open(f"{path}/script.sql", "r", encoding="utf-8").read()).to_df()


def write_to_file(row):
    with open(f"{path}/scripts/{row["table_name"]}.sql", "w", encoding="utf-8") as f:
        f.write(
            row["scripts"]
            # transpile(row["scripts"], read="teradata", write="teradata", pretty=True)[0]
        )


scripts.apply(write_to_file, axis=1)

shutil.make_archive("scripts", "zip", f"{path}/scripts")
