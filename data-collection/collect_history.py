import csv
import os
import sys
import time
import json
import requests
from opensky_api import OpenSkyApi, TokenManager
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# Mod de rulare: python collect_history.py full
#                python collect_history.py daily  (default)
# ─────────────────────────────────────────────
MODE = sys.argv[1] if len(sys.argv) > 1 else "daily"
if MODE not in ("full", "daily"):
    print(f"Mod necunoscut: {MODE}. Foloseste 'full' sau 'daily'.")
    sys.exit(1)

print(f"\n  Mod rulare: {MODE.upper()}")


script_dir = os.path.dirname(os.path.abspath(__file__))
cred_path  = os.path.join(script_dir, '..', 'credentials.json')

with open(cred_path, 'r') as f:
    creds = json.load(f)

CLIENT_ID     = creds.get('clientId')
CLIENT_SECRET = creds.get('clientSecret')

BASE_URL  = "https://opensky-network.org/api"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

api = OpenSkyApi(token_manager=TokenManager.from_json_file(cred_path))

AIRPORTS    = ['LROP', 'EGLL', 'LFPG', 'EDDF', 'EHAM']
SLEEP_SEC   = 15
MAX_RETRIES = 3


DAYS_BACK = 69 if MODE == "full" else 1

fields = [
    "icao24", "callsign", "airport", "type",
    "firstSeen", "lastSeen",
    "arrival_hour", "day_of_week",
    "estDepartureAirport", "estArrivalAirport",
    "date"
]

os.makedirs(os.path.join(script_dir, '..', 'data'), exist_ok=True)
output_path = os.path.join(script_dir, '..', 'data', 'history_traffic.csv')


_token        = None
_token_expiry = None

def get_token():
    global _token, _token_expiry
    if _token and _token_expiry and datetime.now() < _token_expiry:
        return _token
    r = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    })
    r.raise_for_status()
    data          = r.json()
    _token        = data["access_token"]
    _token_expiry = datetime.now() + timedelta(seconds=data.get("expires_in", 1800) - 30)
    return _token

def auth_headers():
    return {"Authorization": f"Bearer {get_token()}"}


def check_credits():
    now      = int(datetime.utcnow().timestamp())
    begin_ts = now - 3600
    url      = f"{BASE_URL}/flights/arrival"
    params   = {"airport": "EDDF", "begin": begin_ts, "end": now}

    try:
        r           = requests.get(url, headers=auth_headers(), params=params, timeout=15)
        remaining   = r.headers.get("X-Rate-Limit-Remaining", "N/A")
        retry_after = r.headers.get("X-Rate-Limit-Retry-After-Seconds", None)

        print(f"\n{'='*50}")
        print(f"  STATUS CREDITE (bucket /flights/*):")
        print(f"  HTTP Status:     {r.status_code}")
        print(f"  Credite ramase:  {remaining}")

        if retry_after:
            retry_min = int(retry_after) // 60
            print(f"  Retry dupa:      {retry_after}s (~{retry_min} minute)")
            print(f"{'='*50}\n")
            return 0, int(retry_after)

        print(f"{'='*50}\n")
        remaining_int = int(remaining) if remaining != "N/A" else None
        return remaining_int, None

    except Exception as e:
        print(f"  [!] Nu am putut verifica creditele: {e}")
        return None, None


def fetch_with_retry(fn, *args):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = fn(*args)
            return result if result else []
        except Exception as e:
            err_str = str(e)
            if "429" in err_str:
                print(f"\n  [!] CREDITE EPUIZATE (429)!")
                _, retry_after = check_credits()
                if retry_after:
                    print(f"  Astept {retry_after}s ({retry_after//60} minute)...")
                    time.sleep(retry_after + 5)
                    continue
            print(f"  [!] Eroare (incercare {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_SEC * attempt)
    return []


def extract_flight(f, airport, flight_type, target_date):
    last_seen  = getattr(f, 'lastSeen', None)
    first_seen = getattr(f, 'firstSeen', None)

    if last_seen:
        dt           = datetime.utcfromtimestamp(last_seen)
        arrival_hour = dt.hour
        day_of_week  = dt.weekday()
    else:
        arrival_hour = None
        day_of_week  = None

    return {
        "icao24":              getattr(f, 'icao24', ''),
        "callsign":            getattr(f, 'callsign', ''),
        "airport":             airport,
        "type":                flight_type,
        "firstSeen":           first_seen,
        "lastSeen":            last_seen,
        "arrival_hour":        arrival_hour,
        "day_of_week":         day_of_week,
        "estDepartureAirport": getattr(f, 'estDepartureAirport', ''),
        "estArrivalAirport":   getattr(f, 'estArrivalAirport', ''),
        "date":                target_date.strftime('%Y-%m-%d')
    }

def load_existing_dates():
    """Citeste CSV-ul existent si returneaza multimea de date deja colectate."""
    if not os.path.exists(output_path):
        return set()
    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return set(row['date'] for row in reader)

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

# 1. Verifica credite
print("Verific creditele disponibile...")
credits_available, retry_after = check_credits()

end_date = datetime.utcnow() - timedelta(days=1)  # intotdeauna incepem de ieri

if retry_after:
    print(f"Credite epuizate! Incearca din nou dupa {retry_after // 60} minute.")
    sys.exit(1)


if credits_available is not None:
    # Calculam cate zile noi ar trebui colectate (fara cele existente)
    existing_dates = load_existing_dates()
    zile_noi = sum(
        1 for i in range(DAYS_BACK)
        if (end_date - timedelta(days=i)).strftime('%Y-%m-%d') not in existing_dates
    )
    
    cost_estimat = zile_noi * len(AIRPORTS) * 2 * 30
    print(f"Credite disponibile:  {credits_available}")
    print(f"Zile noi de colectat: {zile_noi}")
    print(f"Cost estimat real:    {cost_estimat} credite")

    if credits_available < cost_estimat:
        zile_posibile = credits_available // (len(AIRPORTS) * 2 * 30)
        print(f"\n  [!] ATENTIE: Credite insuficiente!")
        print(f"  Poti colecta maxim {zile_posibile} zile noi.")
        if zile_posibile == 0:
            print("  Nu sunt suficiente credite. Opresc.")
            sys.exit(1)
# 2. In modul daily, verifica daca ziua de ieri e deja in CSV


if MODE == "daily":
    existing_dates = load_existing_dates()
    yesterday = end_date.strftime('%Y-%m-%d')
    if yesterday in existing_dates:
        print(f"  Ziua {yesterday} e deja in CSV. Nimic de facut.")
        sys.exit(0)
    print(f"  Adaug ziua: {yesterday}\n")


all_data = []
errors   = []

for i in range(DAYS_BACK):
    target_date = end_date - timedelta(days=i)
    date_str    = target_date.strftime('%Y-%m-%d')

    # In modul full, sarim peste zilele deja existente
    if MODE == "full":
        existing_dates = load_existing_dates()
        if date_str in existing_dates:
            print(f"\n  [skip] {date_str} deja in CSV.")
            continue

    begin_ts = int(target_date.replace(hour=0,  minute=0,  second=0).timestamp())
    end_ts   = int(target_date.replace(hour=23, minute=59, second=59).timestamp())

    print(f"\n[{i+1}/{DAYS_BACK}] Colectez pentru {date_str} ...")

    for airport in AIRPORTS:

        # Sosiri
        print(f"  -> {airport} sosiri ...", end=" ", flush=True)
        arrivals = fetch_with_retry(api.get_arrivals_by_airport, airport, begin_ts, end_ts)
        if arrivals:
            for f in arrivals:
                all_data.append(extract_flight(f, airport, "arrival", target_date))
            print(f"{len(arrivals)} zboruri")
        else:
            print("0 zboruri / eroare")
            errors.append((date_str, airport, "arrival"))
        time.sleep(SLEEP_SEC)

        # Plecari
        print(f"  -> {airport} plecari ...", end=" ", flush=True)
        departures = fetch_with_retry(api.get_departures_by_airport, airport, begin_ts, end_ts)
        if departures:
            for f in departures:
                all_data.append(extract_flight(f, airport, "departure", target_date))
            print(f"{len(departures)} zboruri")
        else:
            print("0 zboruri / eroare")
            errors.append((date_str, airport, "departure"))
        time.sleep(SLEEP_SEC)

if all_data:
    file_exists = os.path.exists(output_path)
    write_mode  = 'a' if file_exists else 'w'

    with open(output_path, write_mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        if not file_exists:
            writer.writeheader()  
        writer.writerows(all_data)

    print(f"\n{'='*50}")
    print(f"  {'Adaugat' if file_exists else 'Creat'}: {len(all_data)} inregistrari -> {output_path}")
    print(f"  din care sosiri:  {sum(1 for r in all_data if r['type'] == 'arrival')}")
    print(f"  din care plecari: {sum(1 for r in all_data if r['type'] == 'departure')}")
else:
    print("\n  Nicio inregistrare noua de salvat.")

if errors:
    print(f"\n  Requesturi esuate ({len(errors)}):")
    for e in errors:
        print(f"    {e[0]} | {e[1]} | {e[2]}")
else:
    print("\n  Nicio eroare!")

print("\nVerific creditele ramase...")
check_credits()
