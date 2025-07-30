from confluent_kafka import Consumer
import json
import pandas as pd
from collections import defaultdict, deque
from statistics import mean, stdev
from datetime import datetime
import os
import csv

# Output folder
output_dir = "rolling_stats"
os.makedirs(output_dir, exist_ok=True)

# Kafka setup
TOPICS = ['yellow-taxi', 'fhvhv-taxi']
BOOTSTRAP_SERVERS = 'broker1-kr:9092'

def create_consumer():
    return Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'rolling-stats',
        'auto.offset.reset': 'earliest'
    })

# Lookup zone ID → borough
zone_df = pd.read_csv('zone_lookup.csv')
location_to_borough = dict(zip(zone_df['LocationID'], zone_df['Borough']))

# Rolling window setup
ROLLING_SIZE = 100
attributes = ['trip_distance', 'passenger_count', 'total_amount']
windows = defaultdict(lambda: defaultdict(lambda: deque(maxlen=ROLLING_SIZE)))

# Define consistent column order
CSV_COLUMNS = [
    'timestamp', 'source', 'borough',
    'trip_distance_mean', 'trip_distance_std', 'trip_distance_count',
    'passenger_count_mean', 'passenger_count_std', 'passenger_count_count',
    'total_amount_mean', 'total_amount_std', 'total_amount_count'
]

def process(record):
    try:
        loc_id = int(record.get('pulocationid', -1))
        borough = location_to_borough.get(loc_id, 'Unknown')
    except:
        borough = 'Unknown'

    for attr in attributes:
        val = float(record.get(attr, 0.0) or 0.0)
        windows[borough][attr].append(val)

    output = {
        'borough': borough
    }
    for attr in attributes:
        values = list(windows[borough][attr])
        output[f"{attr}_mean"] = mean(values)
        output[f"{attr}_std"] = stdev(values) if len(values) > 1 else 0.0
        output[f"{attr}_count"] = len(values)
    return output

if __name__ == "__main__":
    consumer = create_consumer()
    consumer.subscribe(TOPICS)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            # Parse and process
            record = json.loads(msg.value().decode('utf-8'))
            result = process(record)
            pickup_time = record.get('pickup_datetime') or record.get('tpep_pickup_datetime') or record.get('request_datetime')
            result['timestamp'] = pickup_time if pickup_time else datetime.utcnow().isoformat()

            result['source'] = msg.topic()

            # Fill any missing columns
            for col in CSV_COLUMNS:
                if col not in result:
                    result[col] = 0.0 if '_mean' in col or '_std' in col else 0

            # Save per borough
            borough = result['borough']
            file_path = os.path.join(output_dir, f"{borough}.csv")
            file_exists = os.path.isfile(file_path)

            with open(file_path, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(result)

            print(result)

    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        consumer.close()
