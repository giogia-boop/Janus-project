# fetch_all.py
import requests
import time
import json
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import re

# === CONFIG: lista delle stazioni inner layer (usa i link che mi hai dato) ===
STATIONS = [
    {"id":"grezzana", "name":"Grezzana (ARPAV)", "url":"https://wwwold.arpa.veneto.it/bollettini/meteo/h24/img12/Mappa_TEMP.htm?x=24060", "type":"arpav"},
    {"id":"marzana", "name":"Marzana", "url":"https://stazioni.meteoproject.it/dati/marzana/", "type":"meteoproject"},
    {"id":"torricelle", "name":"Torricelle", "url":"https://stazioni.meteoproject.it/dati/torricelle/", "type":"meteoproject"},
    {"id":"gazzego", "name":"Contrada Gazzego", "url":"https://evrgreen.it/", "type":"evrgreen"},
    {"id":"belvedere", "name":"Via Belvedere", "url":"https://evrgreen.it/", "type":"evrgreen"},
    {"id":"biancospini", "name":"Via dei Biancospini", "url":"https://evrgreen.it/", "type":"evrgreen"},
    {"id":"montorio", "name":"Montorio", "url":"https://www.montorioveronese.it/template/indexDesktop.php", "type":"montorio"},
    {"id":"antonio_legnago", "name":"Via Antonio da Legnago", "url":"https://evrgreen.it/", "type":"evrgreen"},
    {"id":"borgo_venezia", "name":"Borgo Venezia", "url":"https://www.meteonlinebvvr.altervista.org/", "type":"meteonline"},
    {"id":"forte_san_mattia", "name":"Forte San Mattia", "url":"https://evrgreen.it/", "type":"evrgreen"},
]

HEADERS = {"User-Agent": "JanusBot/1.0 (+https://github.com/yourusername/janus)"}

# === helper per parsare pagine MeteoProject (tabelle) ===
def parse_table_to_last_row(soup):
    table = soup.find("table")
    if not table:
        return None
    # headers
    ths = table.find_all("th")
    headers = [th.get_text(strip=True).lower() for th in ths] if ths else []
    # rows
    trs = table.find_all("tr")
    data_rows = []
    for tr in trs[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        cols = [td.get_text(strip=True) for td in tds]
        # align with headers
        row = {}
        for i, v in enumerate(cols):
            key = headers[i] if i < len(headers) else f"col{i}"
            row[key] = v
        data_rows.append(row)
    if not data_rows:
        return None
    return data_rows[0]  # first data row (o -1 per ultima riga)

def to_number(s):
    if s is None: 
        return None
    s = s.strip().replace(",", ".")
    m = re.search(r"-?\d+(\.\d+)?", s)
    return float(m.group(0)) if m else None

def fetch_meteoproject(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        row = parse_table_to_last_row(soup)
        if not row:
            # fallback: prendi il testo grezzo
            text = soup.get_text(" ", strip=True)
            return {"raw_preview": text[:500]}
        # normalizza colonne comuni
        data = {}
        # timestamp
        for k in ("data","ora","timestamp","time"):
            if k in row:
                data["timestamp"] = row[k]
                break
        # temperatura
        for c in ("temperatura","temp","t"):
            if c in row:
                data["temperature"] = to_number(row[c])
                break
        # umidità
        for c in ("umidità","umidita","rhum","rh"):
            if c in row:
                data["humidity"] = to_number(row[c])
                break
        # pioggia
        for c in ("pioggia","precipitazione","rain","prcp"):
            if c in row:
                data["rain_mm"] = to_number(row[c])
                break
        # vento
        for c in ("vento","wind","ws"):
            if c in row:
                data["wind"] = row[c]
                break
        data["raw_row"] = row
        return data
    except Exception as e:
        return {"error": str(e)}

def fetch_generic(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        text = r.text
        preview = BeautifulSoup(text, "lxml").get_text(" ", strip=True)[:600]
        return {"raw_preview": preview}
    except Exception as e:
        return {"error": str(e)}

# dispatcher
def fetch_station(st):
    typ = st.get("type","generic")
    url = st["url"]
    if typ == "meteoproject":
        return fetch_meteoproject(url)
    # per gli altri (ARPAV, EVRGREEN, ecc.) per ora generic
    return fetch_generic(url)

def main():
    out = {"generated_at": datetime.now(timezone.utc).isoformat(), "interval_minutes":15, "stations":{}}
    for s in STATIONS:
        print("Fetching", s["id"], s["url"])
        try:
            res = fetch_station(s)
        except Exception as e:
            res = {"error":"exception: "+str(e)}
        res["fetched_at"] = datetime.now(timezone.utc).isoformat()
        res["url"] = s["url"]
        out["stations"][s["id"]] = {"meta": {"name": s["name"]}, "data": res}
        time.sleep(1.5)  # rispetta i server
    # salva file JSON
    with open("docs/data/dati.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("Wrote docs/data/dati.json")

if __name__ == "__main__":
    main()

