import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Docker Kafka port
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample data
event_types = ['view', 'add_to_cart', 'purchase']

# Simulate "popular product" (to trigger your condition)
HOT_PRODUCTS = [1, 2]   # these will get more views

def generate_event():
    product_id = random.randint(1, 10)

    # Make some products get more views but fewer purchases
    if product_id in HOT_PRODUCTS:
        event_type = random.choices(
            ['view', 'add_to_cart', 'purchase'],
            weights=[80, 15, 5]   # mostly views
        )[0]
    else:
        event_type = random.choice(event_types)

    event = {
        "user_id": random.randint(1, 100),
        "product_id": product_id,
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat()
    }

    return event


def main():
    print("Starting Kafka Producer...")

    while True:
        data = generate_event()

        producer.send('clickstream', value=data)

        print(f"Sent: {data}")

        time.sleep(1)   # 1 event per second


if __name__ == "__main__":
    main()