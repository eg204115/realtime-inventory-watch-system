import json
import random
import signal
import sys
import time
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer

# ---------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------

KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "clickstream-events"

EVENT_TYPES = ["view", "add_to_cart", "purchase"]

# Weighted behavior
EVENT_WEIGHTS = [0.75, 0.20, 0.05]

PRODUCT_IDS = [
    "iphone_18",
    "samsung_s24",
    "airpods_pro",
    "gaming_mouse",
    "mechanical_keyboard",
    "macbook_air_m3",
    "ipad_pro",
    "smart_watch",
    "monitor_4k",
    "gaming_laptop"
]

# Products that will intentionally get
# many views but very low purchases
HIGH_INTEREST_PRODUCTS = [
    "iphone_18",
    "gaming_laptop"
]

fake = Faker()

running = True

# ---------------------------------------------------
# GRACEFUL SHUTDOWN
# ---------------------------------------------------

def shutdown_handler(sig, frame):
    global running
    print("\n🛑 Stopping producer gracefully...")
    running = False

signal.signal(signal.SIGINT, shutdown_handler)

# ---------------------------------------------------
# KAFKA PRODUCER
# ---------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10,
    acks="all"
)

# ---------------------------------------------------
# EVENT GENERATION
# ---------------------------------------------------

def generate_event():
    """
    Generate realistic ecommerce clickstream event.
    """

    product_id = random.choice(PRODUCT_IDS)

    # ---------------------------------------------------
    # Controlled low-conversion behavior
    # ---------------------------------------------------

    if product_id in HIGH_INTEREST_PRODUCTS:
        event_type = random.choices(
            ["view", "add_to_cart", "purchase"],
            weights=[0.90, 0.08, 0.02],
            k=1
        )[0]
    else:
        event_type = random.choices(
            EVENT_TYPES,
            weights=EVENT_WEIGHTS,
            k=1
        )[0]

    event = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "product_id": product_id,
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "session_id": fake.uuid4(),
        "device_type": random.choice(
            ["mobile", "desktop", "tablet"]
        ),
        "location": random.choice(
            ["Sri Lanka", "India", "Singapore", "UAE"]
        )
    }

    return event

# ---------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------

def main():

    print("=" * 60)
    print("🚀 Ecommerce Clickstream Producer Started")
    print(f"📡 Kafka Broker : {KAFKA_BROKER}")
    print(f"📌 Topic         : {TOPIC_NAME}")
    print("=" * 60)

    message_count = 0

    while running:

        try:
            event = generate_event()

            producer.send(TOPIC_NAME, value=event)

            message_count += 1

            print(
                f"[{message_count}] "
                f"{event['event_type'].upper():12} | "
                f"Product: {event['product_id']:20} | "
                f"User: {event['user_id']}"
            )

            # Simulate streaming behavior
            time.sleep(random.uniform(0.2, 1.0))

        except Exception as e:
            print(f"❌ Error sending event: {e}")

    producer.flush()
    producer.close()

    print("✅ Producer stopped successfully")


# ---------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------

if __name__ == "__main__":
    main()