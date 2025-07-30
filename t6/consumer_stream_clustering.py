from kafka import KafkaConsumer
import json, time
from dateutil.parser import parse
import numpy as np
from sklearn.cluster import MiniBatchKMeans
from collections import deque

# Parameters
TOPICS = ['yellow_stream', 'fhvhv_stream']
BOOTSTRAP_SERVERS = 'localhost:9092'
WINDOW_SECONDS = 3600  # one-hour sliding window
BATCH_SIZE = 200
K = 5

# Initialize Kafka consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
)

# Sliding window and k-means model
events = deque()
kmeans = MiniBatchKMeans(n_clusters=K, batch_size=BATCH_SIZE)
batch_data = []

def to_ts(dt_str):
    return parse(dt_str).timestamp()

for msg in consumer:
    record = msg.value
    # Current time for window management
    current_time = time.time()

    # Extract features: distance, fare, passenger count, duration, tip
    pickup = parse(record['pickup_datetime'])
    dropoff = parse(record['dropoff_datetime'])
    duration = (dropoff - pickup).total_seconds() / 60
    features = np.array([
        record['trip_distance'],
        record['fare_amount'],
        record['passenger_count'],
        duration,
        record.get('tip_amount', 0.0)
    ])

    # Append to sliding window
    events.append((current_time, features))
    while events and events[0][0] < current_time - WINDOW_SECONDS:
        events.popleft()

    # Batch update for streaming k-means
    batch_data.append(features)
    if len(batch_data) >= BATCH_SIZE:
        data = np.vstack(batch_data)
        kmeans.partial_fit(data)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] k-means centroids:\n{kmeans.cluster_centers_}")
        batch_data.clear()