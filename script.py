import sys
import csv
import requests
import time
import argparse
import mysql.connector
import os
import configparser 

GREEN = "\033[92m"
RESET = "\033[0m"
CSV_FILE = "geocoded.csv"
CONFIG_FILE = "config.ini" 

DEFAULT_KEYS = [
    "lat", "lon", "formatted", "name", "housenumber", "street", "postcode",
    "district", "suburb", "country_code", "country", "state_code", "city",
    "state", "confidence", "confidence_street_level", "confidence_building_level",
    "confidence_city_level", "reverse"
]

# --- Funzioni invariate: get_nominatim_data, write_to_geocoded_csv, process_file ---

def get_nominatim_data(address: str):
    """Esegue la geocodifica di un indirizzo utilizzando l'API Nominatim di OpenStreetMap."""
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address, "format": "json", "addressdetails": 1}
    headers = {"User-Agent": "Geo_noAPI_Script_v1.0"} 
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status() 
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
                "city": address_data.get("city", "") or address_data.get("town", "") or address_data.get("village", ""), 
                "state": address_data.get("state", ""),
                "confidence": str(result.get("importance", 0.0)),
                "confidence_street_level": "", 
                "confidence_building_level": "",
                "confidence_city_level": "",
                "reverse": ""
            }
        return None
    except requests.exceptions.RequestException as e:
        print(f"Errore nella richiesta Nominatim: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Errore imprevisto in get_nominatim_data: {e}", file=sys.stderr)
    return None


def write_to_geocoded_csv(headers, rows):
    """Scrive i dati geocodificati in un file CSV, includendo l'header solo la prima volta."""
    write_header = not os.path.isfile(CSV_FILE) or os.path.getsize(CSV_FILE) == 0
    with open(CSV_FILE, "a", newline='', encoding="utf-8") as f: 
        writer = csv.writer(f)
        if write_header:
            writer.writerow(headers)
        writer.writerows(rows)


def process_file(file_path: str):
    """Legge indirizzi da un file CSV, li geocodifica e salva i risultati in un nuovo CSV."""
    output_rows = []
    try:
        with open(file_path, newline='', encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                headers = next(reader)
            except StopIteration:
                print(f"File vuoto: {file_path}")
                return
            
            output_headers = headers + DEFAULT_KEYS
            
            for row in reader:
                if not row or not row[0].strip():
                    continue
                address = row[0].strip()
                data = get_nominatim_data(address)
                
                extended_row = list(row) 
                
                if data:
                    print(GREEN + f"{address} -> {data['lat']}, {data['lon']} (nominatim)" + RESET)
                    extended_row.extend([data.get(k, "") for k in DEFAULT_KEYS])
                else:
                    print(f"{address} -> Coordinate non trovate")
                    extended_row.extend(["0.0", "0.0"] + [""] * (len(DEFAULT_KEYS) - 2))
                    
                output_rows.append(extended_row)
                time.sleep(1)
                
        write_to_geocoded_csv(output_headers, output_rows)
        
    except FileNotFoundError:
        print(f"Errore: File non trovato: {file_path}", file=sys.stderr)
    except Exception as e:
        print(f"Si è verificato un errore durante l'elaborazione del file: {e}", file=sys.stderr)

# --- Funzione update_mysql_records MODIFICATA ---

def update_mysql_records(config: configparser.ConfigParser):
    """Si connette a MySQL utilizzando la configurazione (inclusa la porta), geocodifica i record mancanti e aggiorna il database."""
    
    # Estrai i dati di connessione dalla sezione 'mysql'
    try:
        db_host = config.get('mysql', 'host')
        db_user = config.get('mysql', 'user')
        db_password = config.get('mysql', 'password')
        db_database = config.get('mysql', 'database')
        update_table = config.get('mysql', 'table') 
        
        # NUOVO: Legge la porta, usando 3306 come default se non specificata o se non è un numero
        db_port = config.getint('mysql', 'port', fallback=3306)
        
    except configparser.Error as e:
        print(f"Errore nel file di configurazione: {e}. Assicurati che la sezione [mysql] e le chiavi siano corrette.", file=sys.stderr)
        return

    # Logica precedente per il parsing host:port (mantenuta per retrocompatibilità, 
    # ma la chiave 'port' è più pulita)
    if ":" in db_host:
        try:
            parts = db_host.split(":")
            db_host = parts[0]
            # Sovrascrive la porta se specificata in host:port
            db_port = int(parts[1]) 
        except (ValueError, IndexError):
            print(f"Errore: Formato host non valido: {db_host}. Usare 'indirizzo' o 'indirizzo:porta'.")
            return
    
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(
            host=db_host,
            # USA LA PORTA LETTA DAL CONFIG O DEFAULT
            port=db_port, 
            user=db_user,
            password=db_password,
            database=db_database
        )
        
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(f"""
            SELECT CODICE, INDIRIZZO, Indirizzo2, Città, CAP, Prov, Naz
            FROM {update_table}
            WHERE Latitude IS NULL OR Latitude = 0.0 OR Longitude IS NULL OR Longitude = 0.0
        """)
        records = cursor.fetchall()
    
        output_rows = []
        headers = ["CODICE", "INDIRIZZO", "Indirizzo2", "Città", "CAP", "Prov", "Naz"] + DEFAULT_KEYS
    
        for record in records:
            codice = record["CODICE"]
            address_parts = [record['INDIRIZZO'], record.get('Indirizzo2', '')]
            address_parts.extend([record['CAP'], record['Città'], record.get('Prov', ''), record.get('Naz', '')])
            
            full_address = ", ".join(filter(None, (p.strip() for p in address_parts)))
            
            data = get_nominatim_data(full_address)
            
            if data:
                print(GREEN + f"{codice} -> {data['lat']}, {data['lon']} (nominatim)" + RESET)
                
                update_values = (
                    data["lat"], data["lon"], data["formatted"], data["name"],
                    data["housenumber"], data["street"], data["postcode"], data["district"],
                    data["suburb"], data["country_code"], data["country"], data["state_code"],
                    data["city"], data["state"], data["confidence"],
                    data["confidence_street_level"], data["confidence_building_level"],
                    data["confidence_city_level"], data["reverse"], codice
                )
                
                cursor.execute(f"""
                    UPDATE {update_table} SET
                        Latitude = %s, Longitude = %s, formatted = %s, name = %s,
                        housenumber = %s, street = %s, postcode = %s, district = %s,
                        suburb = %s, country_code = %s, country = %s, state_code = %s,
                        city = %s, state = %s, confidence = %s,
                        confidence_street_level = %s, confidence_building_level = %s,
                        confidence_city_level = %s, Reverse = %s
                    WHERE CODICE = %s
                """, update_values)
                
                output_rows.append([
                    codice, record["INDIRIZZO"], record["Indirizzo2"], record["Città"],
                    record["CAP"], record["Prov"], record["Naz"]
                ] + [data.get(k, "") for k in DEFAULT_KEYS])
            else:
                print(f"{codice} -> Coordinate non trovate")
                output_rows.append([
                    codice, record["INDIRIZZO"], record["Indirizzo2"], record["Città"],
                    record["CAP"], record["Prov"], record["Naz"]
                ] + ["0.0", "0.0"] + [""] * (len(DEFAULT_KEYS) - 2))

            time.sleep(1)
    
        write_to_geocoded_csv(headers, output_rows)
        conn.commit()
        
    except mysql.connector.Error as err:
        print(f"Errore di connessione o query MySQL: {err}", file=sys.stderr)
        
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def print_help():
    """Stampa l'aiuto per l'utilizzo dello script."""
    print(f"""
Utilizzo dello script:

  podman run --rm -v "$PWD/indirizzi.csv:/app/indirizzi.csv" -v "$PWD:/app" maps-scraper --file indirizzi.csv
      ↳ Elabora un file CSV.

  podman run --rm -v "$PWD/{CONFIG_FILE}:/app/{CONFIG_FILE}" -v "$PWD:/app" maps-scraper --config {CONFIG_FILE}
      ↳ Connette al database MySQL usando il file di configurazione (inclusa la porta) e aggiorna la tabella.

  podman run --rm maps-scraper "Via Roma, Milano" "Piazza Duomo, Firenze"
      ↳ Geolocalizza indirizzi diretti.

Note:
  - Per MySQL, assicurati che la porta (default 3306) sia corretta nel file di configurazione montato.
""")


def main():
    """Funzione principale per l'analisi degli argomenti e l'esecuzione delle operazioni."""
    parser = argparse.ArgumentParser(description="Geolocalizza indirizzi con Nominatim") 
    parser.add_argument("--file", help="Percorso del file CSV/TXT con gli indirizzi")
    parser.add_argument("--config", help=f"Percorso del file di configurazione MySQL (es. {CONFIG_FILE})")
    parser.add_argument("addresses", nargs="*", help="Indirizzi passati direttamente")
    
    if len(sys.argv) == 1:
        print_help()
        return

    args = parser.parse_args()

    if args.file:
        process_file(args.file)
    elif args.config:
        config = configparser.ConfigParser()
        if not os.path.isfile(args.config):
            print(f"Errore: File di configurazione non trovato: {args.config}", file=sys.stderr)
            return
            
        config.read(args.config)
        
        # Verifica che la sezione 'mysql' esista prima di tentare di leggerla
        if 'mysql' not in config:
            print(f"Errore: La sezione [mysql] non è presente nel file di configurazione {args.config}.", file=sys.stderr)
            return

        update_mysql_records(config)
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
                output_rows.append([address] + [data.get(k, "") for k in DEFAULT_KEYS])
            else:
                print(f"{address} -> Coordinate non trovate")
                output_rows.append([address] + ["0.0", "0.0"] + [""] * (len(DEFAULT_KEYS) - 2))
            
            time.sleep(1)
        write_to_geocoded_csv(headers, output_rows)

if __name__ == "__main__":
    main()
