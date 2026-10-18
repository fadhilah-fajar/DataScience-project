import requests
import json

URL = "https://api.inaturalist.org/v1/observations"

params = {
    "iconic_taxa[]": "Animalia",

    # Batas wilayah Indonesia
    "swlat": -11,
    "swlng": 95,
    "nelat": 6,
    "nelng": 141,

    # Ambil tepat maksimal 200 data
    "per_page": 200,
    "page": 1
}

print("Mengambil data iNaturalist...")

response = requests.get(
    URL,
    params=params,
    timeout=30
)

print("Status:", response.status_code)

if response.status_code == 200:

    result = response.json()

    observations = result.get("results", [])

    print("Jumlah data:", len(observations))

    all_data = []

    for obs in observations:

        taxon = obs.get("taxon") or {}

        location = obs.get("location")

        latitude = None
        longitude = None

        if location:
            coordinates = location.split(",")

            if len(coordinates) == 2:
                latitude = coordinates[0]
                longitude = coordinates[1]

        data = {
            "observation_id": obs.get("id"),
            "scientific_name": taxon.get("name"),
            "taxon_id": taxon.get("id"),
            "observed_on": obs.get("observed_on"),
            "latitude": latitude,
            "longitude": longitude,
            "quality_grade": obs.get("quality_grade"),
            "place_guess": obs.get("place_guess"),
            "iconic_taxon_name": taxon.get("iconic_taxon_name")
        }

        all_data.append(data)

    # Simpan data mentah hasil ekstraksi
    with open(
        "inaturalist_observations.json",
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            all_data,
            file,
            ensure_ascii=False,
            indent=2
        )

    print("\n==============================")
    print("Pengambilan selesai")
    print("Total data:", len(all_data))
    print("File: inaturalist_observations.json")
    print("==============================")

else:

    print("Gagal mengambil data.")
    print("Response:")
    print(response.text)