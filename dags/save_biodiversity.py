import requests
import json

url = "https://api.gbif.org/v1/occurrence/search"

params = {
    "country": "ID",
    "kingdomKey": 1,
    "hasCoordinate": "true",
    "limit": 200
}

response = requests.get(url, params=params)

print("Status:", response.status_code)

data = response.json()

print("Jumlah data dari API:", len(data["results"]))

records = []

for item in data["results"]:

    record = {
        "gbif_id": item.get("key"),
        "taxon_key": item.get("taxonKey"),
        "scientific_name": item.get("scientificName"),
        "kingdom": item.get("kingdom"),
        "phylum": item.get("phylum"),
        "class": item.get("class"),
        "order": item.get("order"),
        "family": item.get("family"),
        "genus": item.get("genus"),
        "basis_of_record": item.get("basisOfRecord"),
        "occurrence_status": item.get("occurrenceStatus"),
        "country": item.get("country"),
        "latitude": item.get("decimalLatitude"),
        "longitude": item.get("decimalLongitude"),
        "event_date": item.get("eventDate")
    }

    records.append(record)


with open("biodiversity_animals.json", "w", encoding="utf-8") as file:
    json.dump(records, file, ensure_ascii=False, indent=4)


print("Jumlah data disimpan:", len(records))
print("File berhasil dibuat: biodiversity_animals.json")