import json
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


# =========================================================
# KONFIGURASI
# =========================================================

# Lokasi file JSON di dalam container Airflow
BASE_DIR = Path("/usr/local/airflow/dags/data")

GBIF_FILE = BASE_DIR / "biodiversity_animals.json"
INAT_FILE = BASE_DIR / "inaturalist_observations.json"
NASA_FILE = BASE_DIR / "nasa_power_raw.json"

# Airflow Connection ID
MYSQL_CONN_ID = "mysql-biodiversity"


# =========================================================
# 1. CREATE TABLE
# =========================================================

def create_tables():

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    conn = hook.get_conn()
    cursor = conn.cursor()

    # -----------------------------------------------------
    # TABEL 1 - GBIF
    # -----------------------------------------------------

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS biodiversity_animals (
            gbif_id BIGINT PRIMARY KEY,
            taxon_key BIGINT,
            scientific_name VARCHAR(255),
            kingdom VARCHAR(100),
            phylum VARCHAR(100),
            class_name VARCHAR(100),
            order_name VARCHAR(100),
            family VARCHAR(100),
            genus VARCHAR(100),
            basis_of_record VARCHAR(100),
            occurrence_status VARCHAR(50),
            country VARCHAR(100),
            latitude DOUBLE,
            longitude DOUBLE,
            event_date DATE
        )
    """)

    # -----------------------------------------------------
    # TABEL 2 - iNaturalist
    # -----------------------------------------------------

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inaturalist_observations (
            observation_id BIGINT PRIMARY KEY,
            scientific_name VARCHAR(255),
            taxon_id BIGINT,
            observed_on DATE,
            latitude DOUBLE,
            longitude DOUBLE,
            quality_grade VARCHAR(50),
            place_guess VARCHAR(500),
            iconic_taxon_name VARCHAR(100)
        )
    """)

    # -----------------------------------------------------
    # TABEL 3 - NASA POWER
    # -----------------------------------------------------

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nasa_power_environment (
            id INT AUTO_INCREMENT PRIMARY KEY,
            latitude DOUBLE,
            longitude DOUBLE,
            date DATE,
            temperature DOUBLE,
            precipitation DOUBLE,
            humidity DOUBLE,
            wind_speed DOUBLE,

            UNIQUE KEY unique_location_date (
                latitude,
                longitude,
                date
            )
        )
    """)

    conn.commit()

    cursor.close()
    conn.close()

    print("======================================")
    print("Semua tabel berhasil dibuat / tersedia.")
    print("======================================")


# =========================================================
# 2. EXTRACT + TRANSFORM + LOAD GBIF
# =========================================================

def load_gbif():

    # -----------------------------------------------------
    # EXTRACT
    # -----------------------------------------------------

    if not GBIF_FILE.exists():
        raise FileNotFoundError(
            f"File GBIF tidak ditemukan: {GBIF_FILE}"
        )

    with open(GBIF_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, list):
        raise ValueError(
            "Format biodiversity_animals.json harus berupa list."
        )

    print("======================================")
    print(f"GBIF: {len(data)} data ditemukan dari JSON.")
    print("======================================")

    if len(data) == 0:
        raise ValueError(
            "File biodiversity_animals.json kosong."
        )

    # -----------------------------------------------------
    # TRANSFORM
    # -----------------------------------------------------

    records = []

    for item in data:

        records.append((
            item.get("gbif_id"),
            item.get("taxon_key"),
            item.get("scientific_name"),
            item.get("kingdom"),
            item.get("phylum"),
            item.get("class"),
            item.get("order"),
            item.get("family"),
            item.get("genus"),
            item.get("basis_of_record"),
            item.get("occurrence_status"),
            item.get("country"),
            item.get("latitude"),
            item.get("longitude"),
            item.get("event_date")
        ))

    print(
        f"GBIF: {len(records)} record siap dimasukkan."
    )

    # -----------------------------------------------------
    # LOAD
    # -----------------------------------------------------

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    conn = hook.get_conn()
    cursor = conn.cursor()

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
            event_date = VALUES(event_date)
    """

    cursor.executemany(sql, records)

    conn.commit()

    print("======================================")
    print(
        f"GBIF: {len(records)} data berhasil "
        f"diproses ke MySQL."
    )
    print("======================================")

    cursor.close()
    conn.close()


# =========================================================
# 3. EXTRACT + TRANSFORM + LOAD iNATURALIST
# =========================================================

def load_inaturalist():

    # -----------------------------------------------------
    # EXTRACT
    # -----------------------------------------------------

    if not INAT_FILE.exists():
        raise FileNotFoundError(
            f"File iNaturalist tidak ditemukan: {INAT_FILE}"
        )

    with open(INAT_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, list):
        raise ValueError(
            "Format inaturalist_observations.json harus berupa list."
        )

    print("======================================")
    print(
        f"iNaturalist: {len(data)} data ditemukan dari JSON."
    )
    print("======================================")

    if len(data) == 0:
        raise ValueError(
            "File inaturalist_observations.json kosong."
        )

    # -----------------------------------------------------
    # TRANSFORM
    # -----------------------------------------------------

    records = []

    for item in data:

        records.append((
            item.get("observation_id"),
            item.get("scientific_name"),
            item.get("taxon_id"),
            item.get("observed_on"),
            item.get("latitude"),
            item.get("longitude"),
            item.get("quality_grade"),
            item.get("place_guess"),
            item.get("iconic_taxon_name")
        ))

    print(
        f"iNaturalist: {len(records)} record siap dimasukkan."
    )

    # -----------------------------------------------------
    # LOAD
    # -----------------------------------------------------

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    conn = hook.get_conn()
    cursor = conn.cursor()

    # -----------------------------------------------------
    # FULL REFRESH
    #
    # Data lama dihapus terlebih dahulu.
    #
    # Tujuannya agar tabel MySQL selalu mencerminkan
    # isi JSON iNaturalist terbaru.
    #
    # Ini penting karena sebelumnya terdapat data
    # iNaturalist dari luar Indonesia.
    # -----------------------------------------------------

    print(
        "iNaturalist: membersihkan data lama..."
    )

    cursor.execute("""
        TRUNCATE TABLE inaturalist_observations
    """)

    # -----------------------------------------------------
    # INSERT DATA TERBARU
    # -----------------------------------------------------

    sql = """
        INSERT INTO inaturalist_observations (
            observation_id,
            scientific_name,
            taxon_id,
            observed_on,
            latitude,
            longitude,
            quality_grade,
            place_guess,
            iconic_taxon_name
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
    """

    cursor.executemany(sql, records)

    conn.commit()

    print("======================================")
    print(
        f"iNaturalist: {len(records)} data berhasil "
        f"dimuat ke MySQL."
    )
    print("Mode: FULL REFRESH")
    print("======================================")

    cursor.close()
    conn.close()


# =========================================================
# 4. EXTRACT + TRANSFORM + LOAD NASA POWER
# =========================================================

def load_nasa_power():

    # -----------------------------------------------------
    # EXTRACT
    # -----------------------------------------------------

    if not NASA_FILE.exists():
        raise FileNotFoundError(
            f"File NASA POWER tidak ditemukan: {NASA_FILE}"
        )

    with open(NASA_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)

    print("======================================")
    print("NASA POWER: JSON berhasil dibaca.")
    print("======================================")

    # -----------------------------------------------------
    # VALIDASI STRUKTUR JSON
    # -----------------------------------------------------

    if "properties" not in data:
        raise ValueError(
            "JSON NASA POWER tidak memiliki 'properties'."
        )

    if "parameter" not in data["properties"]:
        raise ValueError(
            "JSON NASA POWER tidak memiliki 'parameter'."
        )

    if "geometry" not in data:
        raise ValueError(
            "JSON NASA POWER tidak memiliki 'geometry'."
        )

    # -----------------------------------------------------
    # TRANSFORM
    # -----------------------------------------------------

    parameters = data["properties"]["parameter"]

    temperatures = parameters.get("T2M", {})
    precipitation = parameters.get("PRECTOTCORR", {})
    humidity = parameters.get("RH2M", {})
    wind_speed = parameters.get("WS2M", {})

    # -----------------------------------------------------
    # KOORDINAT NASA POWER
    #
    # Struktur:
    #
    # [longitude, latitude, elevation]
    # -----------------------------------------------------

    coordinates = data["geometry"]["coordinates"]

    if len(coordinates) < 2:
        raise ValueError(
            "Koordinat NASA POWER tidak valid."
        )

    longitude = coordinates[0]
    latitude = coordinates[1]

    print(
        f"NASA POWER latitude  : {latitude}"
    )

    print(
        f"NASA POWER longitude : {longitude}"
    )

    # -----------------------------------------------------
    # MEMBENTUK RECORD
    # -----------------------------------------------------

    records = []

    dates = temperatures.keys()

    for date_str in dates:

        date_value = datetime.strptime(
            date_str,
            "%Y%m%d"
        ).date()

        records.append((
            latitude,
            longitude,
            date_value,
            temperatures.get(date_str),
            precipitation.get(date_str),
            humidity.get(date_str),
            wind_speed.get(date_str)
        ))

    print(
        f"NASA POWER: {len(records)} data siap "
        f"dimasukkan ke MySQL."
    )

    if len(records) == 0:
        raise ValueError(
            "Tidak ada data temperatur NASA POWER."
        )

    # -----------------------------------------------------
    # LOAD
    # -----------------------------------------------------

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    conn = hook.get_conn()
    cursor = conn.cursor()

    sql = """
        INSERT INTO nasa_power_environment (
            latitude,
            longitude,
            date,
            temperature,
            precipitation,
            humidity,
            wind_speed
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            temperature = VALUES(temperature),
            precipitation = VALUES(precipitation),
            humidity = VALUES(humidity),
            wind_speed = VALUES(wind_speed)
    """

    cursor.executemany(sql, records)

    conn.commit()

    print("======================================")
    print(
        f"NASA POWER: {len(records)} data berhasil "
        f"diproses ke MySQL."
    )
    print("======================================")

    cursor.close()
    conn.close()


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="etl_biodiversity",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "biodiversity",
        "etl",
        "mysql"
    ]
) as dag:

    # -----------------------------------------------------
    # TASK 1
    # -----------------------------------------------------

    create_table = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )

    # -----------------------------------------------------
    # TASK 2
    # -----------------------------------------------------

    extract_load_gbif = PythonOperator(
        task_id="load_gbif",
        python_callable=load_gbif
    )

    # -----------------------------------------------------
    # TASK 3
    # -----------------------------------------------------

    extract_load_inaturalist = PythonOperator(
        task_id="load_inaturalist",
        python_callable=load_inaturalist
    )

    # -----------------------------------------------------
    # TASK 4
    # -----------------------------------------------------

    extract_load_nasa = PythonOperator(
        task_id="load_nasa_power",
        python_callable=load_nasa_power
    )

    # -----------------------------------------------------
    # DEPENDENCY
    # -----------------------------------------------------

    create_table >> [
        extract_load_gbif,
        extract_load_inaturalist,
        extract_load_nasa
    ]