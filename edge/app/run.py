import json
import logging
import os
import random
import time

import paho.mqtt.publish as publish

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

BROKER = os.environ.get("MQTT_BROKER", "mqtt")
PORT   = int(os.environ.get("MQTT_PORT", 1883))
TOPIC  = "herdmind/sensors"
INTERVAL = int(os.environ.get("PUBLISH_INTERVAL", 30))
HERD = [101, 102, 103, 104, 105]

def make_reading(cow_id):
    return {
        "cow_id": cow_id,
        "temperature":   round(random.uniform(38.3, 39.2), 2),
        "rumination":    round(random.uniform(260, 320)),
        "activity":      round(random.uniform(55, 90)),
        "milk_yield":    round(random.uniform(19, 25), 1),
        "conductivity":  round(random.uniform(4.5, 5.8), 2),
        "flow_rate":     round(random.uniform(2.2, 2.9), 2),
        "quarter_delta": round(random.uniform(0.2, 0.7), 2),
        "lying_time":    round(random.uniform(10, 14), 1),
        "heart_rate":    round(random.uniform(65, 78)),
    }

def main():
    log.info("Edge starting — broker=%s:%d", BROKER, PORT)
    tick = 0
    while True:
        for cow_id in HERD:
            reading = make_reading(cow_id)
            try:
                publish.single(TOPIC, json.dumps(reading), hostname=BROKER, port=PORT)
                log.info("cow_id=%s temp=%.1f rum=%.0f", cow_id, reading["temperature"], reading["rumination"])
            except Exception as exc:
                log.error("Publish failed: %s", exc)
        tick += 1
        log.info("tick %d — sleeping %ds", tick, INTERVAL)
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
