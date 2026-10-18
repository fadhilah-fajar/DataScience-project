import requests

url = "https://api.gbif.org/v1/occurrence/search"

params = {
    "country": "ID",
    "kingdomKey": 1,
    "limit": 10
}

response = requests.get(url, params=params)

print("Status:", response.status_code)

data = response.json()

print("Jumlah data:", len(data["results"]))

for i, item in enumerate(data["results"], start=1):
    print(f"\n--- Data {i} ---")

    print("GBIF ID          :", item.get("key"))
    print("Taxon Key        :", item.get("taxonKey"))
    print("Nama ilmiah      :", item.get("scientificName"))

    print("Kingdom          :", item.get("kingdom"))
    print("Phylum           :", item.get("phylum"))
    print("Class            :", item.get("class"))
    print("Order            :", item.get("order"))
    print("Family           :", item.get("family"))
    print("Genus            :", item.get("genus"))

    print("Basis of Record  :", item.get("basisOfRecord"))
    print("Occurrence Status:", item.get("occurrenceStatus"))

    print("Country          :", item.get("country"))
    print("Latitude         :", item.get("decimalLatitude"))
    print("Longitude        :", item.get("decimalLongitude"))

    print("Has Coordinate   :", item.get("hasCoordinate"))
    print("Event Date       :", item.get("eventDate"))