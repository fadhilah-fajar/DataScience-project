import requests

url = "https://api.inaturalist.org/v1/places"

params = {
    "q": "Indonesia",
    "per_page": 10
}

response = requests.get(url, params=params, timeout=30)

print("Status:", response.status_code)

if response.status_code == 200:
    data = response.json()

    for place in data.get("results", []):
        print(
            "ID:", place.get("id"),
            "| Nama:", place.get("name"),
            "| Tipe:", place.get("place_type_name")
        )
else:
    print(response.text)