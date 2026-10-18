import json
import pandas as pd
import os

input_file = r"C:\Users\LENOVO\airflow-docker\airflow-astro\dags\data\nasa_power_environment.json"
output_file = r"C:\Users\LENOVO\airflow-docker\airflow-astro\dags\data\nasa_power_environment.csv"

# Cek apakah file JSON ada
if not os.path.exists(input_file):
    print("File JSON tidak ditemukan!")
    print(input_file)
    exit()

# Baca JSON
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# Ubah ke DataFrame
df = pd.DataFrame(data)

# Simpan sebagai CSV
df.to_csv(output_file, index=False, encoding="utf-8-sig")

print("Konversi berhasil!")
print("Jumlah data :", len(df))
print("Jumlah kolom:", len(df.columns))

print("\nKolom:")
print(df.columns.tolist())

print("\n5 data pertama:")
print(df.head())

print("\nFile CSV tersimpan di:")
print(output_file)