# Aggiunto Dockerhub
https://hub.docker.com/r/lscanferlato/

# maps-scraper
Servizio geocoding Nominatim
Esempio uso

podman build -t maps-scraper .

docker run --rm maps-scraper "Piazza San Marco Venezia" "Colosseo Roma"

# con file
podman run --rm -v $(pwd):/app maps-scraper --file indirizzi.csv
