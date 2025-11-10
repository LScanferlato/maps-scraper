import sys
import csv
import requests
import time
import argparse
import mysql.connector
import os

GREEN = "\033[92m"
RESET = "\033[0m"
CSV_FILE = "geocoded.csv"

DEFAULT_KEYS = [
    "lat", "lon", "formatted", "name", "housenumber", "street", "postcode",
    "district", "suburb", "country_code", "country", "state_code", "city",
    "state", "confidence", "confidence_street_level", "confidence_building_level",
    "confidence_city_level", "reverse"
]

def get_nominatim_data(address: str):
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address, "format": "json", "addressdetails": 1}
    headers = {"User-Agent": "Geo_noAPI"}
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        data = response.json()
        if data:
            result = data[0]
            lat = result.get("lat", "0.0")
            lon = result.get("lon", "0.0")
            formatted = result.get("display_name", "")
            address_data = result.get("address", {})

            return {
                "lat": lat,
                "lon": lon,
                "formatted": formatted,
                "name": address_data.get("name", ""),
                "housenumber": address_data.get("house_number", ""),
                "street": address_data.get("road", ""),
                "postcode": address_data.get("postcode", ""),
                "district": address_data.get("district", ""),
                "suburb": address_data.get("suburb", ""),
                "country_code": address_data.get("country_code", ""),
                "country": address_data.get("country", ""),
                "state_code": address_data.get("state_code", ""),
                "city": address_data.get("city", ""),
                "state": address_data.get("state", ""),
                "confidence": result.get("importance", ""),
                "confidence_street_level": "",
                "confidence_building_level": "",
                "confidence_city_level": "",
                "reverse": ""
            }
    except Exception:
        pass
    return None

def write_to_geocoded_csv(headers, rows):
    write_header = not os.path.isfile(CSV_FILE) or os.path.getsize(CSV_FILE) == 0
    with open(CSV_FILE, "a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(headers)
        writer.writerows(rows)

def process_file(file_path: str):
    output_rows = []
    with open(file_path, newline='', encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        for row in reader:
            if not row or not row[0].strip():
                continue
            address = row[0].strip()
            data = get_nominatim_data(address)

            if data:
                print(GREEN + f"{address} -> {data['lat']}, {data['lon']} (nominatim)" + RESET)
                row.extend([data.get(k, "") for k in DEFAULT_KEYS])
            else:
                print(f"{address} -> Coordinate non trovate")
                row.extend(["0.0", "0.0"] + [""] * (len(DEFAULT_KEYS) - 2))
            output_rows.append(row)
            time.sleep(1)

    write_to_geocoded_csv(headers + DEFAULT_KEYS, output_rows)

def update_mysql_records(host, user, password, database):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT CODICE, INDIRIZZO, Indirizzo2, Città, CAP, Prov, Naz
        FROM indirizzi
        WHERE Latitude IS NULL OR Latitude = 0.0 OR Longitude IS NULL OR Longitude = 0.0
    """)
    records = cursor.fetchall()

    output_rows = []
    headers = ["CODICE", "INDIRIZZO", "Indirizzo2", "Città", "CAP", "Prov", "Naz"] + DEFAULT_KEYS

    for record in records:
        codice = record["CODICE"]
        full_address = f"{record['INDIRIZZO']} {record['Indirizzo2'] or ''}, {record['CAP']} {record['Città']}, {record['Prov']}, {record['Naz']}"
        data = get_nominatim_data(full_address)

        if data:
            print(GREEN + f"{codice} -> {data['lat']}, {data['lon']} (nominatim)" + RESET)
            cursor.execute("""
                UPDATE indirizzi SET
                    Latitude = %s, Longitude = %s, formatted = %s, name = %s,
                    housenumber = %s, street = %s, postcode = %s, district = %s,
                    suburb = %s, country_code = %s, country = %s, state_code = %s,
                    city = %s, state = %s, confidence = %s,
                    confidence_street_level = %s, confidence_building_level = %s,
                    confidence_city_level = %s, Reverse = %s
                WHERE CODICE = %s
            """, (
                data["lat"], data["lon"], data["formatted"], data["name"],
                data["housenumber"], data["street"], data["postcode"], data["district"],
                data["suburb"], data["country_code"], data["country"], data["state_code"],
                data["city"], data["state"], data["confidence"],
                data["confidence_street_level"], data["confidence_building_level"],
                data["confidence_city_level"], data["reverse"], codice
            ))
            output_rows.append([
                codice, record["INDIRIZZO"], record["Indirizzo2"], record["Città"],
                record["CAP"], record["Prov"], record["Naz"]
            ] + [data.get(k, "") for k in DEFAULT_KEYS])
        else:
            print(f"{codice} -> Coordinate non trovate")
        time.sleep(1)

    write_to_geocoded_csv(headers, output_rows)
    conn.commit()
    cursor.close()
    conn.close()

def print_help():
    print("""
Utilizzo dello script:

  podman run --rm -v "$PWD/indirizzi.csv:/app/indirizzi.csv" -v "$PWD:/app" maps-scraper --file indirizzi.csv
      ↳ Elabora un file CSV montato nel container e scrive/aggiorna geocoded.csv nella directory locale.

  podman run --rm maps-scraper --sql HOST USER PASSWORD DATABASE
      ↳ Connette al database MySQL e aggiorna la tabella 'indirizzi', scrivendo anche su geocoded.csv.

  podman run --rm maps-scraper "Via Roma, Milano" "Piazza Duomo, Firenze"
      ↳ Mostra in output le coordinate e i dettagli per indirizzi diretti e li salva in geocoded.csv.

Note:
  - Assicurati che il file CSV sia montato correttamente con -v "$PWD/indirizzi.csv:/app/indirizzi.csv"
  - Il file geocoded.csv verrà creato o aggiornato nella directory locale grazie al volume -v "$PWD:/app"
""")

def main():
    parser = argparse.ArgumentParser(description="Geolocalizza indirizzi con Nominatim", add_help=False)
    parser.add_argument("--file", help="Percorso del file CSV/TXT con gli indirizzi")
    parser.add_argument("--sql", nargs=4, metavar=("HOST", "USER", "PASSWORD", "DB"),
                        help="Credenziali MySQL: host user password database")
    parser.add_argument("addresses", nargs="*", help="Indirizzi passati direttamente")
    args = parser.parse_args()

    if args.file:
        process_file(args.file)
    elif args.sql:
        host, user, password, db = args.sql
        update_mysql_records(host, user, password, db)
    elif args.addresses:
        headers = ["Indirizzo"] + DEFAULT_KEYS
        output_rows = []
        for address in args.addresses:
            if not address.strip():
                continue
            address = address.strip()
            data = get_nominatim_data(address)
            if data:
                print(GREEN + f"{address} -> {data['lat']}, {data['lon']} (nominatim)" + RESET)
                for k, v in data.items():
                    print(f"{k}: {v}")
                output_rows.append([address] + [data.get(k, "") for k in DEFAULT_KEYS])
            else:
                print(f"{address} -> Coordinate non trovate")
                output_rows.append([address] + ["0.0", "0.0"] + [""] * (len(DEFAULT_KEYS) - 2))
            time.sleep(1)
        write_to_geocoded_csv(headers, output_rows)
    else:
        print_help()

if __name__ == "__main__":
    main()
