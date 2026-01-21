import pandas as pd
import psycopg2
from psycopg2 import extras

DB_CONFIG = {
    "host": "10.119.50.152",
    "port": 5432,
    "dbname": "stagingdb",
    "user": "admindb",
    "password": "admin"
}

CSV_FILE_PATH = "DAFTAR_PEGAWAI.csv"
TABLE_NAME = "daftar_pegawai"

CSV_COLUMNS_TO_DB = [
    "NIK_KARYAWAN",
    "PERUSAHAAN",
    "HIRE_DATE",
    "TERMINATION_DATE",
    "STATUS"
]




def upload_csv_to_postgres(csv_path, table_name, db_config, columns):
    try:
       
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
            print("CSV file kosong, tidak ada data untuk diupload.")
            return

        missing_cols = set(columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Kolom tidak ditemukan di CSV: {missing_cols}")


        df = df[columns]

        records = [tuple(row) for row in df.itertuples(index=False, name=None)]
        column_str = ",".join(columns)

        insert_query = f"""
            INSERT INTO daftar_pegawai (nik,company,hire,termination,status)
            VALUES %s
        """

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute("TRUNCATE TABLE daftar_pegawai")
        extras.execute_values(cursor, insert_query, records)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Berhasil upload {len(records)} rows ke tabel daftar_pegawai")

    except Exception as e:
        print("Terjadi error:", e)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    upload_csv_to_postgres(
        csv_path=CSV_FILE_PATH,
        table_name=TABLE_NAME,
        db_config=DB_CONFIG,
        columns=CSV_COLUMNS_TO_DB
    )


