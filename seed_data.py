import time
import json
import random
import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "herd/sensors/telemetry"

def generate_cow_telemetry(cow_id):
    is_sick = random.random() < 0.10
    
    if is_sick:
        temperature = round(random.uniform(39.6, 41.5), 2)
        rumination = round(random.uniform(100.0, 250.0), 1)
        activity = round(random.uniform(10.0, 45.0), 1)
        conductivity = round(random.uniform(8.5, 12.0), 2)  # Infection marker
        lying_time = round(random.uniform(14.5, 19.0), 1)
    else:
        temperature = round(random.uniform(38.2, 39.3), 2)
        rumination = round(random.uniform(400.0, 550.0), 1)
        activity = round(random.uniform(70.0, 120.0), 1)
        conductivity = round(random.uniform(4.0, 6.0), 2)
        lying_time = round(random.uniform(9.0, 13.0), 1)
        
    return {
        "cow_id": cow_id,
        "temperature": temperature,
        "rumination": rumination,
        "activity": activity,
        "milk_yield": round(random.uniform(18.0, 26.0), 1),
        "conductivity": conductivity,
        "flow_rate": round(random.uniform(2.0, 3.5), 1),
        "quarter_delta": round(random.uniform(0.2, 0.7), 2),
        "lying_time": lying_time,
        "heart_rate": round(random.uniform(65.0, 80.0), 1)
    }

def main():
    print(f"📡 Initializing Mock Telemetry Engine... Connecting to MQTT Broker ({MQTT_BROKER}:{MQTT_PORT})")
    client = mqtt.Client(client_id="external_herd_generator")
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"❌ Failed to reach MQTT broker. Error: {e}")
        return

    tracked_cow_ids = [101, 102, 103, 104, 105]
    print(f"🚀 Telemetry streaming initialized for herd IDs: {tracked_cow_ids}. Press Ctrl+C to stop.\n")

    while True:
        for cow_id in tracked_cow_ids:
            payload = generate_cow_telemetry(cow_id)
            client.publish(MQTT_TOPIC, json.dumps(payload))
            print(f"📤 Sent: Cow {payload['cow_id']} | Temp: {payload['temperature']}°C | Rum: {payload['rumination']} min | Cnd: {payload['conductivity']}")
        time.sleep(5)

if __name__ == "__main__":
    main()
