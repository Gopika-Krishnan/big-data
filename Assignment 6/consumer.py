# consumer_ordered.py

from confluent_kafka import Consumer
import json
import pandas as pd
from datetime import datetime

# Kafka config
BOOTSTRAP_SERVERS = 'broker1-kr:9092'
TOPIC = 'yellow-taxi'
GROUP_ID = 'basic-ordered-consumer'

# Create Kafka consumer
def create_consumer():
    return Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

# Parse pickup time from record
def get_pickup_time(record):
    for key in ['pickup_datetime', 'tpep_pickup_datetime', 'request_datetime']:
        if key in record:
            try:
                return pd.to_datetime(record[key])
            except:
                pass
    return datetime.utcnow()

if __name__ == "__main__":
    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    print(f"Consuming from topic: {TOPIC}")

    buffer = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value().decode('utf-8'))
                pickup_time = get_pickup_time(record)
                buffer.append((pickup_time, record))

                # Process in sorted order every 100 records
                if len(buffer) >= 100:
                    buffer.sort(key=lambda x: x[0])
                    for time, rec in buffer:
                        print(f"{time} | {rec}")
                    buffer.clear()

            except Exception as e:
                print(f"Failed to process message: {e}")

    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        consumer.close()
