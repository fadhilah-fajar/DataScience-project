from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime
import requests


# =========================================================
# 1. CREATE TABLE
# =========================================================

def create_table():
    hook = MySqlHook(mysql_conn_id="mysql-biodiversity")

    sql = """
    CREATE TABLE IF NOT EXISTS biodiversity_animals (
        gbif_id BIGINT PRIMARY KEY,
        taxon_key BIGINT,
        scientific_name VARCHAR(255),
        kingdom VARCHAR(50),
        phylum VARCHAR(100),
        class_name VARCHAR(100),
        order_name VARCHAR(100),
        family VARCHAR(100),
        genus VARCHAR(100),
        basis_of_record VARCHAR(100),
        occurrence_status VARCHAR(50),
        country VARCHAR(100),
        latitude DECIMAL(10,7),
        longitude DECIMAL(10,7),
        event_date DATETIME NULL
    );
    """

    hook.run(sql)

    print("Tabel biodiversity_animals berhasil dibuat/dicek.")


# =========================================================
# 2. EXTRACT + TRANSFORM
# =========================================================

def extract_transform(**context):

    url = "https://api.gbif.org/v1/occurrence/search"

    params = {
        "country": "ID",
        "kingdomKey": 1,
        "hasCoordinate": "true",
        "limit": 200
    }

    response = requests.get(url, params=params, timeout=60)

    response.raise_for_status()

    data = response.json()

    print("Status API:", response.status_code)
    print("Jumlah data dari API:", len(data["results"]))

    records = []

    for item in data["results"]:

        event_date = item.get("eventDate")

        # Mengubah eventDate menjadi format DATETIME MySQL
        if event_date:
            try:
                event_date = datetime.fromisoformat(
                    event_date.replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except ValueError:
                event_date = None

        record = (
            item.get("key"),
            item.get("taxonKey"),
            item.get("scientificName"),
            item.get("kingdom"),
            item.get("phylum"),
            item.get("class"),
            item.get("order"),
            item.get("family"),
            item.get("genus"),
            item.get("basisOfRecord"),
            item.get("occurrenceStatus"),
            item.get("country"),
            item.get("decimalLatitude"),
            item.get("decimalLongitude"),
            event_date
        )

        records.append(record)

    print("Jumlah data setelah transform:", len(records))

    # Kirim data ke task berikutnya menggunakan XCom
    context["ti"].xcom_push(
        key="biodiversity_records",
        value=records
    )

    return len(records)


# =========================================================
# 3. LOAD
# =========================================================

def load_to_mysql(**context):

    hook = MySqlHook(mysql_conn_id="mysql-biodiversity")

    records = context["ti"].xcom_pull(
        task_ids="extract_transform",
        key="biodiversity_records"
    )

    if not records:
        print("Tidak ada data untuk dimasukkan.")
        return

    sql = """
    INSERT INTO biodiversity_animals (
        gbif_id,
        taxon_key,
        scientific_name,
        kingdom,
        phylum,
        class_name,
        order_name,
        family,
        genus,
        basis_of_record,
        occurrence_status,
        country,
        latitude,
        longitude,
        event_date
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        taxon_key = VALUES(taxon_key),
        scientific_name = VALUES(scientific_name),
        kingdom = VALUES(kingdom),
        phylum = VALUES(phylum),
        class_name = VALUES(class_name),
        order_name = VALUES(order_name),
        family = VALUES(family),
        genus = VALUES(genus),
        basis_of_record = VALUES(basis_of_record),
        occurrence_status = VALUES(occurrence_status),
        country = VALUES(country),
        latitude = VALUES(latitude),
        longitude = VALUES(longitude),
        event_date = VALUES(event_date);
    """

    connection = hook.get_conn()
    cursor = connection.cursor()

    cursor.executemany(sql, records)

    connection.commit()

    cursor.close()
    connection.close()

    print(f"{len(records)} data berhasil di-load ke MySQL.")


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="etl_biodiversity_to_mysql",
    start_date=datetime(2026, 9, 22),
    schedule="@daily",
    catchup=False,
    tags=["biodiversity", "gbif", "mysql", "etl"],
) as dag:

    create_table_mysql = PythonOperator(
        task_id="create_table_mysql",
        python_callable=create_table
    )

    extract_transform = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform
    )

    load_to_mysql = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load_to_mysql
    )

    create_table_mysql >> extract_transform >> load_to_mysql