import pandas as pd
from pathlib import Path


# =========================================================
# KONFIGURASI
# =========================================================

BASE_DIR = Path(
    r"C:\Users\LENOVO\airflow-docker\airflow-astro\dags\data"
)


# =========================================================
# LOAD CSV
# =========================================================

gbif = pd.read_csv(
    BASE_DIR / "biodiversity_animals.csv"
)

inat = pd.read_csv(
    BASE_DIR / "inaturalist_observations.csv"
)

nasa = pd.read_csv(
    BASE_DIR / "nasa_power_environment.csv"
)


# =========================================================
# 1. UKURAN DATA
# =========================================================

print("\n======================================")
print("UKURAN DATA")
print("======================================")

print("GBIF          :", gbif.shape)
print("iNaturalist   :", inat.shape)
print("NASA POWER    :", nasa.shape)


# =========================================================
# 2. NAMA KOLOM
# =========================================================

print("\n======================================")
print("KOLOM GBIF")
print("======================================")

print(gbif.columns.tolist())


print("\n======================================")
print("KOLOM iNATURALIST")
print("======================================")

print(inat.columns.tolist())


print("\n======================================")
print("KOLOM NASA POWER")
print("======================================")

print(nasa.columns.tolist())


# =========================================================
# 3. JUMLAH NULL
# =========================================================

print("\n======================================")
print("NULL GBIF")
print("======================================")

print(gbif.isnull().sum())


print("\n======================================")
print("NULL iNATURALIST")
print("======================================")

print(inat.isnull().sum())


print("\n======================================")
print("NULL NASA POWER")
print("======================================")

print(nasa.isnull().sum())


# =========================================================
# 4. DUPLIKAT
# =========================================================

print("\n======================================")
print("DUPLIKAT")
print("======================================")

print("GBIF          :", gbif.duplicated().sum())
print("iNaturalist   :", inat.duplicated().sum())
print("NASA POWER    :", nasa.duplicated().sum())


# =========================================================
# 5. JUMLAH ID UNIK
# =========================================================

print("\n======================================")
print("ID UNIK")
print("======================================")

print(
    "GBIF ID unik        :",
    gbif["gbif_id"].nunique()
)

print(
    "iNaturalist ID unik :",
    inat["observation_id"].nunique()
)


# =========================================================
# 6. VALIDASI KOORDINAT
# =========================================================

print("\n======================================")
print("VALIDASI KOORDINAT")
print("======================================")

print("GBIF:")
print(
    "Latitude minimum :",
    gbif["latitude"].min()
)

print(
    "Latitude maksimum:",
    gbif["latitude"].max()
)

print(
    "Longitude minimum:",
    gbif["longitude"].min()
)

print(
    "Longitude maksimum:",
    gbif["longitude"].max()
)


print("\niNaturalist:")

print(
    "Latitude minimum :",
    inat["latitude"].min()
)

print(
    "Latitude maksimum:",
    inat["latitude"].max()
)

print(
    "Longitude minimum:",
    inat["longitude"].min()
)

print(
    "Longitude maksimum:",
    inat["longitude"].max()
)


print("\nNASA POWER:")

print(
    "Latitude minimum :",
    nasa["latitude"].min()
)

print(
    "Latitude maksimum:",
    nasa["latitude"].max()
)

print(
    "Longitude minimum:",
    nasa["longitude"].min()
)

print(
    "Longitude maksimum:",
    nasa["longitude"].max()
)


# =========================================================
# 7. PREVIEW DATA
# =========================================================

print("\n======================================")
print("5 DATA PERTAMA GBIF")
print("======================================")

print(gbif.head())


print("\n======================================")
print("5 DATA PERTAMA iNATURALIST")
print("======================================")

print(inat.head())


print("\n======================================")
print("5 DATA PERTAMA NASA POWER")
print("======================================")

print(nasa.head())


# =========================================================
# SELESAI
# =========================================================

print("\n======================================")
print("VALIDASI SELESAI")
print("======================================")