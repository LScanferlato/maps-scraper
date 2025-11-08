import sys
import csv
import requests
import time
import argparse
import mysql.connector

GREEN = "\033[92m"
RESET = "\033[0m"

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
                "country": address_data.get("country", ""),
                "country_code": address_data.get("country_code", ""),
                "confidence": result.get("importance", ""),
                "confidence_street_level": "",
                "confidence_building_level": "",
                "confidence_city_level": "",
                "reverse": ""
            }
    except Exception:
        pass
    return None

def process_file(file_path: str):
    output_rows = []
    with open(file_path, newline='', encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        for row in reader:
            address = row[0]
            data = get_nominatim_data(address)

            if data:
                print(GREEN + f"{address} -> {data['lat']}, {data['lon']} (nominatim)" + RESET)
                row.extend([data.get(k, "") for k in data])
            else:
                print(f"{address} -> Coordinate non trovate")
                row.extend(["0.0", "0.0"] + [""] * 21)
            output_rows.append(row)
            time.sleep(1)

    with open("output_" + file_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers + list(data.keys()))
        writer.writerows(output_rows)

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
                    city = %s, state = %s, country = %s, country_code = %s,
                    confidence = %s, confidence_street_level = %s,
                    confidence_building_level = %s, confidence_city_level = %s,
                    Reverse = %s
                WHERE CODICE = %s
            """, (
                data["lat"], data["lon"], data["formatted"], data["name"],
                data["housenumber"], data["street"], data["postcode"], data["district"],
                data["suburb"], data["country_code"], data["country"], data["state_code"],
                data["city"], data["state"], data["country"], data["country_code"],
                data["confidence"], data["confidence_street_level"],
                data["confidence_building_level"], data["confidence_city_level"],
                data["reverse"], codice
            ))
        else:
            print(f"{codice} -> Coordinate non trovate")
        time.sleep(1)

    conn.commit()
    cursor.close()
    conn.close()

def print_help():
    print("""
Utilizzo dello script:

  python script.py --file indirizzi.csv
      ↳ Elabora un file CSV e scrive un file output con coordinate e dettagli.

  python script.py --sql HOST USER PASSWORD DATABASE
      ↳ Aggiorna la tabella 'indirizzi' in MySQL con coordinate e metadati.

  python script.py "Via Roma, Milano" "Piazza Duomo, Firenze"
      ↳ Mostra in output le coordinate e i dettagli per indirizzi diretti.
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
        for address in args.addresses:
            data = get_nominatim_data(address)
            if data:
                print(GREEN + f"{address} -> {data['lat']}, {data['lon']} (nominatim)" + RESET)
                for k, v in data.items():
                    print(f"{k}: {v}")
            else:
                print(f"{address} -> Coordinate non trovate")
            time.sleep(1)
    else:
        print_help()

if __name__ == "__main__":
    main()
