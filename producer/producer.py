import json
import random
import time
import logging
from datetime import datetime

from kafka import KafkaProducer
from faker import Faker

# -----------------------------------------
# Logging Configuration
# -----------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("clickstream-producer")

# -----------------------------------------
# Kafka Configuration
# -----------------------------------------

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "clickstream-events"

# -----------------------------------------
# Initialize Faker
# -----------------------------------------

fake = Faker()

# -----------------------------------------
# Create Kafka Producer
# -----------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------------------
# Sample Product Catalog
# -----------------------------------------

PRODUCTS = [
    {"product_id": "P1001", "category": "Smartphones"},
    {"product_id": "P1002", "category": "Laptops"},
    {"product_id": "P1003", "category": "Headphones"},
    {"product_id": "P1004", "category": "Gaming"},
    {"product_id": "P1005", "category": "Smartwatches"},
    {"product_id": "P1006", "category": "Tablets"},
    {"product_id": "P1007", "category": "Accessories"},
]

# -----------------------------------------
# Event Types with Realistic Probabilities
# -----------------------------------------

EVENT_TYPES = [
    ("view", 75),
    ("add_to_cart", 20),
    ("purchase", 5)
]

# -----------------------------------------
# Function to Generate Event Type
# -----------------------------------------

def generate_event_type():
    events = [e[0] for e in EVENT_TYPES]
    weights = [e[1] for e in EVENT_TYPES]

    return random.choices(events, weights=weights, k=1)[0]

# -----------------------------------------
# Controlled Flash Sale Trigger Logic
# -----------------------------------------

HOT_PRODUCTS = ["P1001", "P1002"]

def generate_product():
    """
    Occasionally generate heavy traffic
    on selected products to trigger
    high-interest low-conversion alerts.
    """

    # 40% chance to use hot products
    if random.random() < 0.4:
        return random.choice(
            [p for p in PRODUCTS if p["product_id"] in HOT_PRODUCTS]
        )

    return random.choice(PRODUCTS)

# -----------------------------------------
# Generate Clickstream Event
# -----------------------------------------

def generate_clickstream_event():

    product = generate_product()

    event_type = generate_event_type()

    # Reduce purchases for hot products
    # to simulate low conversion
    if product["product_id"] in HOT_PRODUCTS:
        if random.random() < 0.85:
            event_type = "view"

    event = {
        "user_id": f"U{random.randint(1000, 9999)}",
        "product_id": product["product_id"],
        "category": product["category"],
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat()
    }

    return event

# -----------------------------------------
# Main Producer Loop
# -----------------------------------------

def start_streaming():

    logger.info("Starting Clickstream Producer...")

    try:

        while True:

            event = generate_clickstream_event()

            producer.send(TOPIC_NAME, value=event)

            logger.info(f"Produced Event: {event}")

            # Random delay for realistic stream
            time.sleep(random.uniform(0.2, 1.0))

    except KeyboardInterrupt:

        logger.warning("Producer stopped manually.")

    finally:

        producer.flush()
        producer.close()

        logger.info("Kafka producer connection closed.")

# -----------------------------------------
# Run Producer
# -----------------------------------------

if __name__ == "__main__":
    start_streaming()