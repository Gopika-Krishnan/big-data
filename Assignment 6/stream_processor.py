from confluent_kafka import Consumer, Producer
import json
import pandas as pd
from collections import deque, defaultdict
import numpy as np
import time
import csv

BOOTSTRAP_SERVERS = 'broker1-kr:9092'
INPUT_TOPIC = 'yellow-taxi'
OUTPUT_TOPIC = 'yellow-taxi-stats'

ROLLING_WINDOW = 100
NUM_CLUSTERS = 3

# Kafka setup
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'python-stats-processor',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([INPUT_TOPIC])

producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

# Load zone -> borough mapping
zone_to_borough = {}
with open("zone_lookup.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        zone_to_borough[int(row["LocationID"])] = row["Borough"]

# Data stores
location_data = defaultdict(lambda: deque(maxlen=ROLLING_WINDOW))
borough_data = defaultdict(lambda: deque(maxlen=ROLLING_WINDOW))
all_stats = {}
borough_stats = {}

# Clustering state
cluster_centers = np.random.rand(NUM_CLUSTERS, 2)
cluster_assignments = {}

def compute_stats(data):
    arr = np.array(data)
    means = np.mean(arr, axis=0)
    stds = np.std(arr, axis=0)
    return means, stds

def normalize(vec):
    return [vec[0]/10.0, vec[1]/50.0]  # Normalize distance, fare

def update_clusters():
    global cluster_centers
    for loc_id, stats in all_stats.items():
        vec = normalize(stats[1:3])
        point = np.array(vec)
        dists = np.linalg.norm(cluster_centers - point, axis=1)
        cluster_id = np.argmin(dists)
        cluster_centers[cluster_id] = 0.9 * cluster_centers[cluster_id] + 0.1 * point
        cluster_assignments[loc_id] = cluster_id

def write_to_kafka(loc_id, means, stds, count):
    payload = {
        "location_id": loc_id,
        "count": count,
        "mean_distance": round(means[0], 2),
        "mean_fare": round(means[1], 2),
        "std_distance": round(stds[0], 2),
        "std_fare": round(stds[1], 2)
    }
    producer.produce(OUTPUT_TOPIC, key=str(loc_id), value=json.dumps(payload))
    producer.flush()

def write_location_stats(filename):
    with open(filename, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["location_id", "count", "mean_distance", "mean_fare", "std_distance", "std_fare", "cluster_id"])
        for loc_id, stats in all_stats.items():
            cluster_id = cluster_assignments.get(loc_id, -1)
            writer.writerow([loc_id] + stats + [cluster_id])

def write_borough_stats(filename):
    with open(filename, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["borough", "count", "mean_distance", "mean_fare", "std_distance", "std_fare"])
        for borough, entries in borough_data.items():
            if not entries:
                continue
            arr = np.array(entries)
            means = np.mean(arr, axis=0)
            stds = np.std(arr, axis=0)
            writer.writerow([borough, len(entries), round(means[0], 2), round(means[1], 2), round(stds[0], 2), round(stds[1], 2)])

def write_top_10(filename):
    top_10 = sorted(all_stats.items(), key=lambda x: x[1][0], reverse=True)[:10]
    with open(filename, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["location_id", "count", "mean_distance", "mean_fare", "std_distance", "std_fare", "cluster_id"])
        for loc_id, stats in top_10:
            cluster_id = cluster_assignments.get(loc_id, -1)
            writer.writerow([loc_id] + stats + [cluster_id])

print("Starting stats processor with borough tracking and clustering...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        try:
            record = json.loads(msg.value().decode('utf-8'))
            loc_id = str(record['pulocationid'])
            features = [float(record['trip_distance']), float(record['fare_amount'])]
            location_data[loc_id].append(features)

            borough = zone_to_borough.get(int(record['pulocationid']), "Unknown")
            borough_data[borough].append(features)

            if len(location_data[loc_id]) >= 10:
                means, stds = compute_stats(location_data[loc_id])
                count = len(location_data[loc_id])
                all_stats[loc_id] = [count, round(means[0], 2), round(means[1], 2), round(stds[0], 2), round(stds[1], 2)]
                write_to_kafka(loc_id, means, stds, count)

        except Exception as e:
            print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print("Shutting down...")

finally:
    update_clusters()
    write_location_stats("rolling_stats_with_clusters.csv")
    write_borough_stats("borough_stats.csv")
    write_top_10("top_10_locations.csv")
    consumer.close()
