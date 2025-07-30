from kafka import KafkaConsumer
import json, time, math
from dateutil.parser import parse
import numpy as np
from collections import namedtuple

# Parameters
TOPICS = ['yellow_stream', 'fhvhv_stream']
BOOTSTRAP_SERVERS = 'localhost:9092'
EPSILON = 0.5
MU = 5
LAMBDA = 0.001

MicroCluster = namedtuple('MicroCluster', ['center', 'weight', 'last_update'])
clusters = []

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
)

def fade(cluster, now):
    dt = now - cluster.last_update
    w = cluster.weight * math.exp(-LAMBDA * dt)
    return MicroCluster(cluster.center, w, now)

def to_features(rec):
    pu = parse(rec['pickup_datetime'])
    do = parse(rec['dropoff_datetime'])
    dur = (do - pu).total_seconds() / 60
    return np.array([
        rec['trip_distance'],
        rec['fare_amount'],
        rec['passenger_count'],
        dur,
        rec.get('tip_amount', 0.0)
    ])

for msg in consumer:
    rec = msg.value
    now = time.time()
    feats = to_features(rec)

    # Fade all existing micro-clusters
    clusters = [fade(c, now) for c in clusters]

    # Assign to nearest micro-cluster or create new one
    if clusters:
        dists = [np.linalg.norm(feats - c.center) for c in clusters]
        idx = int(np.argmin(dists))
        if dists[idx] <= EPSILON:
            c = clusters[idx]
            new_w = c.weight + 1
            new_center = (c.center * c.weight + feats) / new_w
            clusters[idx] = MicroCluster(new_center, new_w, now)
        else:
            clusters.append(MicroCluster(feats, 1.0, now))
    else:
        clusters.append(MicroCluster(feats, 1.0, now))

    # Count cores vs outliers
    cores = [c for c in clusters if c.weight >= MU]
    outliers = [c for c in clusters if c.weight < MU]
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] cores={len(cores)}, outliers={len(outliers)}")

