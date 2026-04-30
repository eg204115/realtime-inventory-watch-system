import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

HOT_PRODUCTS = [1, 2]

def generate_event():
    # Bias product distribution
    product_id = random.choices(
        [1,2,3,4,5,6,7,8,9,10],
        weights=[40,40,5,3,3,3,2,2,1,1]
    )[0]

    # Bias event types
    if product_id in HOT_PRODUCTS:
        event_type = random.choices(
            ['view', 'add_to_cart', 'purchase'],
            weights=[95, 4, 1]
        )[0]
    else:
        event_type = random.choice(['view', 'add_to_cart', 'purchase'])

    return {
        "user_id": random.randint(1, 100),
        "product_id": product_id,
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat()
    }

def main():
    print("High-speed producer started...")

    while True:
        for _ in range(20):  # burst events
            data=generate_event()
            producer.send('clickstream', value=data)
            print(f"Produced: {data}")
        producer.flush()
        time.sleep(0.01)  # small delay

if __name__ == "__main__":
    main()