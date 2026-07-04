import json
import time
import os

from kafka import KafkaProducer
from opensky_api import OpenSkyApi, TokenManager


script_dir = os.path.dirname(os.path.abspath(__file__))
cred_path = os.path.join(script_dir, "..", "credentials.json")
# OpenSky API
api = OpenSkyApi(
    token_manager=TokenManager.from_json_file(cred_path)
)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

print("Producer-ul a pornit și trimite date către Kafka...")

while True:
    try:
        # Europa
        states = api.get_states(bbox=(35, 70, -10, 40))

        if states and states.states:

            for s in states.states:

                flight_data = {

                    # Snapshot
                    "timestamp": states.time,

                    # Identificare
                    "icao24": getattr(s, "icao24", None),
                    "callsign": s.callsign.strip() if getattr(s, "callsign", None) else None,
                    "origin_country": getattr(s, "origin_country", None),

                    # Poziție
                    "latitude": getattr(s, "latitude", None),
                    "longitude": getattr(s, "longitude", None),

                    # Altitudine
                    "baro_altitude": getattr(s, "baro_altitude", None),
                    "geo_altitude": getattr(s, "geo_altitude", None),

                    # Mișcare
                    "velocity": getattr(s, "velocity", None),
                    "true_track": getattr(s, "true_track", None),
                    "vertical_rate": getattr(s, "vertical_rate", None),

                    # Momente de timp
                    "time_position": getattr(s, "time_position", None),
                    "last_contact": getattr(s, "last_contact", None),

                    # Stare
                    "on_ground": getattr(s, "on_ground", None),
                    "position_source": getattr(s, "position_source", None),
                    "category": getattr(s, "category", None),

                    # Transponder
                    "squawk": getattr(s, "squawk", None),
                    "spi": getattr(s, "spi", None)
                }

                producer.send("zboruri", value=flight_data)

            producer.flush()

            print(f"Am trimis datele pentru {len(states.states)} aeronave.")

        else:
            print("Nu s-au primit date de la OpenSky.")

        time.sleep(60)

    except Exception as e:
        print(f"Eroare în producer: {e}")
        time.sleep(10)