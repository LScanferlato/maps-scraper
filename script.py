import sys
import csv
import requests
import time
import argparse

# ANSI colors
GREEN = "\033[92m"
RESET = "\033[0m"

def get_coordinates_from_nominatim(address: str):
    """Recupera lat/lon da Nominatim (OpenStreetMap)."""
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address, "format": "json"}
    headers = {"User-Agent": "Geo_noAPI"}
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        data = response.json()
        if data:
            return data[0]["lat"], data[0]["lon"]
    except Exception:
        pass
    return None, None

def process_file(file_path: str):
    rows = []
    with open(file_path, newline='', encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            address = row[0]
            lat, lon = get_coordinates_from_nominatim(address)

            if lat and lon:
                print(GREEN + f"{address} -> {lat}, {lon} (nominatim)" + RESET)
                row.extend([lat, lon])
            else:
                print(f"{address} -> Coordinate non trovate")
                row.extend(["", ""])
            rows.append(row)
            time.sleep(1)

    # Sovrascrive lo stesso file con le coordinate aggiunte
    with open(file_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

def main():
    parser = argparse.ArgumentParser(description="Ottieni coordinate da indirizzi")
    parser.add_argument("--file", help="Percorso del file CSV/TXT con gli indirizzi")
    parser.add_argument("addresses", nargs="*", help="Indirizzi passati direttamente")
    args = parser.parse_args()

    if args.file:
        process_file(args.file)
    elif args.addresses:
        for address in args.addresses:
            lat, lon = get_coordinates_from_nominatim(address)
            if lat and lon:
                print(GREEN + f"{address} -> {lat}, {lon} (nominatim)" + RESET)
            else:
                print(f"{address} -> Coordinate non trovate")
            time.sleep(1)
    else:
        print("Uso: python script.py --file indirizzi.csv oppure python script.py 'indirizzo1' 'indirizzo2' ...")

if __name__ == "__main__":
    main()
