import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

SOURCE_DB_CONFIG = {
    "host": "10.119.50.152",
    "port": 5432,
    "dbname": "stagingdb",
    "user": "admindb",
    "password": "admin"
}
CSV_OUTPUT_PATH = "exist.csv"

SOURCE_SQL = """
select distinct
    nik,
    company,
    case
	    when hire > termination then termination::date
	    else hire::date
	end as hire,
	case
	    when hire > termination then hire::date
	    else termination::date
	end as termination,
    status
from daftar_pegawai
where nik is not null and (status like '%Exist%' or status is null)
order by hire asc
"""

# =========================
# FUNCTION: SOURCE QUERY → CSV
# =========================
def extract_query_to_csv(sql, csv_path, db_config):
    conn = psycopg2.connect(**db_config)
    df = pd.read_sql(sql, conn)
    conn.close()

    if df.empty:
        print("Query source tidak menghasilkan data.")
        return False

    df.to_csv(csv_path, index=False)
    print(f"Extract {len(df)} rows ke {csv_path}")
    return True

TARGET_DB_CONFIG = {
    "host": "10.119.50.152",
    "port": 5432,
    "dbname": "intermediatedb",
    "user": "admindb",
    "password": "admin"
}
INTERMEDIATE_TABLE = "exist"

def upload_csv_to_postgres(csv_path, table_name, db_config):
    df = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False)

    df = df.replace(
        to_replace=r"^\s*(nan|NULL|null|NaN|NAN)?\s*$",
        value=None,
        regex=True
    )

    df = df.where(pd.notnull(df), None)

    if df.empty:
        print("CSV kosong, skip upload.")
        return

    records = [tuple(row) for row in df.itertuples(index=False, name=None)]
    columns = ",".join(df.columns)

    insert_query = f"""
       INSERT INTO exist (nik,company,hire,termination,status)
            VALUES %s
    """

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE exist")
    execute_values(cursor, insert_query, records)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Upload {len(records)} rows ke tabel {table_name} berhasil")


# =========================
# MAIN PIPELINE
# =========================
if __name__ == "__main__":
    success = extract_query_to_csv(
        sql=SOURCE_SQL,
        csv_path=CSV_OUTPUT_PATH,
        db_config=SOURCE_DB_CONFIG
    )
    if success:
        upload_csv_to_postgres(
            csv_path=CSV_OUTPUT_PATH,
            table_name=INTERMEDIATE_TABLE,
            db_config=TARGET_DB_CONFIG
        )
