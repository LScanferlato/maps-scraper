FROM python:3.11-slim

WORKDIR /app

COPY script.py ./

# Installa le dipendenze necessarie
RUN pip install --no-cache-dir requests beautifulsoup4 lxml

ENTRYPOINT ["python", "script.py"]
