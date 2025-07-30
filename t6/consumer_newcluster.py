from kafka import KafkaConsumer
import json, time
from dateutil.parser import parse
import numpy as np

# Parameters
TOPICS = ['yellow_stream', 'fhvhv_stream']
BOOTSTRAP_SERVERS = 'localhost:9092'
THRESHOLD = 10.0
WINDOW_SECONDS = 3600

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
)

clusters = []

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

    # Assign to existing cluster or create new
    if clusters:
        dists = [np.linalg.norm(feats - c['center']) for c in clusters]
        idx = int(np.argmin(dists))
        if dists[idx] <= THRESHOLD:
            c = clusters[idx]
            c['center'] = (c['center'] * c['count'] + feats) / (c['count'] + 1)
            c['count'] += 1
            c['last_seen'] = now
        else:
            clusters.append({'center': feats, 'count': 1, 'last_seen': now})
    else:
        clusters.append({'center': feats, 'count': 1, 'last_seen': now})

    # Purge old clusters
    clusters = [c for c in clusters if now - c['last_seen'] <= WINDOW_SECONDS]

    # Log current clusters
    centers = np.vstack([c['center'] for c in clusters])
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] clusters={len(clusters)}\n{centers}")
