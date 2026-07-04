# ✈️ Air Traffic Monitoring Platform (OpenSky API)

## Descriere

Acest proiect reprezintă o platformă Big Data pentru monitorizarea traficului aerian european, utilizând date furnizate de **OpenSky Network API**.

Sistemul combină procesarea **Batch** și **Streaming**, permițând atât analiza istorică a traficului aerian, cât și monitorizarea în timp real a aeronavelor aflate în spațiul aerian european.

Arhitectura utilizează:

- Python
- Apache Kafka
- Apache ZooKeeper
- Docker
- OpenSky Network API

---

# Arhitectura Sistemului

Proiectul este împărțit în două componente principale:

## 1. Batch Processing

Responsabilă cu colectarea datelor istorice privind sosirile și plecările pentru aeroporturile monitorizate.

Script:

```
collect_history.py
```

API utilizat:

```
GET /flights/arrival
GET /flights/departure
```

Aceste endpoint-uri sunt apelate pentru fiecare aeroport și pentru fiecare zi analizată.

Datele sunt salvate în:

```
data/history_traffic.csv
```

---

## 2. Streaming Processing

Responsabilă cu monitorizarea traficului aerian în timp real.

Script:

```
producer.py
```

API utilizat:

```
GET /states/all
```

Acest endpoint returnează starea tuturor aeronavelor aflate în bounding box-ul Europei.

Coordonate utilizate:

- Latitudine: **35° – 70°**
- Longitudine: **-10° – 40°**

Datele sunt publicate în Kafka, în topicul:

```
zboruri
```

---

# Componentele proiectului

## 1. Colectarea datelor istorice

Script:

```
collect_history.py
```

Acest script descarcă istoricul zborurilor pentru următoarele aeroporturi:

- LROP – București Otopeni
- EGLL – London Heathrow
- LFPG – Paris Charles de Gaulle
- EDDF – Frankfurt
- EHAM – Amsterdam Schiphol

### Funcționalități

## ✔ Gestionarea limitelor API și optimizarea consumului de credite

Acest modul este optimizat pentru a funcționa eficient cu limitele stricte de credite impuse de **OpenSky Network API** pentru conturile gratuite.

**API-uri utilizate:**

- `api.get_arrivals_by_airport()`
- `api.get_departures_by_airport()`

### Managementul resurselor

- **Estimarea consumului de credite** – înainte de efectuarea interogărilor, scriptul estimează costul acestora pentru a evita depășirea limitelor disponibile.
- **Monitorizarea Rate Limit** – verifică automat header-ele răspunsului API, precum `X-Rate-Limit-Remaining`, pentru a cunoaște numărul de credite rămase.
- **Retry automat** – în cazul în care API-ul răspunde cu eroarea **HTTP 429 (Too Many Requests)**, scriptul respectă timpul indicat în header-ul `Retry-After` și reia automat cererea folosind un mecanism de retry cu backoff progresiv.
- **Protecția creditelor** – dacă numărul de credite disponibile devine insuficient, execuția este oprită preventiv pentru a evita blocarea temporară a contului OpenSky.

### Logica de stocare

Scriptul utilizează un mecanism de **append incremental**, fără a suprascrie istoricul existent.

La fiecare execuție:

1. este încărcat fișierul `data/history_traffic.csv`;
2. sunt identificate zilele deja colectate;
3. sunt interogate doar datele care lipsesc;
4. noile înregistrări sunt adăugate la sfârșitul fișierului.

### Idempotență

Pentru a evita interogările redundante și consumul inutil de credite API, scriptul verifică automat existența datelor înainte de fiecare descărcare. Dacă o zi este deja prezentă în fișierul istoric, aceasta este omisă, fiind descărcate exclusiv datele lipsă.

---

### ✔ Moduri de rulare

#### Daily

Descarcă doar ziua precedentă.

```bash
python collect_history.py daily
```

#### Full

Construiește istoricul complet (ex. ultimele X zile setate în cod), ignorând datele existente.

```bash
python collect_history.py full
```

---

# 2. Infrastructura Kafka

Serviciile sunt definite în:

```
docker-compose.yml
```

## Apache ZooKeeper

- coordonarea brokerului Kafka
- healthcheck pe portul 2181

## Apache Kafka

Broker Kafka disponibil la:

```
localhost:9092
```

Pornirea infrastructurii:

```bash
docker-compose up -d
```

---

# 3. Producer-ul Kafka

Script:

```
producer.py
```

Acesta rulează continuu și execută următorii pași:

1. interoghează OpenSky API la fiecare 60 secunde;
2. filtrează aeronavele aflate în spațiul aerian european;
3. normalizează datele într-un obiect JSON;
4. publică fiecare aeronavă în topicul Kafka **zboruri**.

---

# Structura datelor

## Date istorice

Fișier:

```
history_traffic.csv
```

| Câmp | Tip | Descriere |
|------|-----|-----------|
| icao24 | String | Identificator unic al aeronavei |
| callsign | String | Indicativul zborului |
| airport | String | Aeroport monitorizat |
| type | String | arrival / departure |
| arrival_hour | Integer | Ora producerii evenimentului |
| day_of_week | Integer | Ziua săptămânii (0=Luni) |
| date | Date | Data calendaristică |

---

## Date publicate în Kafka

Fiecare mesaj trimis în topicul **zboruri** conține următoarele câmpuri:

| Câmp | Descriere |
|------|-----------|
| timestamp | Timestamp-ul snapshot-ului OpenSky |
| icao24 | Identificator unic al aeronavei |
| callsign | Indicativul zborului |
| origin_country | Țara de origine |
| latitude | Latitudine |
| longitude | Longitudine |
| baro_altitude | Altitudine barometrică |
| geo_altitude | Altitudine geometrică |
| velocity | Viteza aeronavei (m/s) |
| true_track | Direcția de deplasare (grade) |
| vertical_rate | Viteza de urcare/coborâre (m/s) |
| time_position | Ultimul timestamp al poziției |
| last_contact | Ultimul contact cu aeronava |
| on_ground | Indicator dacă aeronava este la sol |
| position_source | Sursa poziției (ADS-B etc.) |
| category | Categoria aeronavei |
| squawk | Codul transponderului |
| spi | Indicator Special Position Identification |

---

# API-urile utilizate

## collect_history.py

| Endpoint | Scop |
|-----------|------|
| `/flights/arrival` | Obținerea zborurilor sosite într-un aeroport |
| `/flights/departure` | Obținerea zborurilor plecate dintr-un aeroport |

---

## producer.py

| Endpoint | Scop |
|-----------|------|
| `/states/all` | Obținerea poziției și stării tuturor aeronavelor din zona Europei |

---

# 📁 Structura proiectului

```
project/
│
├── data/
│   └── history_traffic.csv
│
├── kafka/
│   └── producer.py
│
├── collect_history.py
├── docker-compose.yml
├── credentials.json
└── README.md
```

---

# 🔑 Configurare

Creați fișierul:

```
credentials.json
```

în directorul principal al proiectului.

```json
{
  "clientId": "utilizator_opensky",
  "clientSecret": "parola_opensky"
}
```

---

# ▶️ Rulare

## Pornirea infrastructurii Kafka

```bash
docker-compose up -d
```

## Construirea istoricului

```bash
python collect_history.py full
```

sau

```bash
python collect_history.py daily
```

## Pornirea producer-ului

```bash
python producer.py
```

Producer-ul va trimite automat un nou snapshot al traficului aerian la fiecare **60 de secunde** în topicul Kafka **zboruri**.

