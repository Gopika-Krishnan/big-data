from confluent_kafka import Consumer
from sklearn.cluster import MiniBatchKMeans
from collections import deque
import numpy as np
import json

BOOTSTRAP_SERVERS = 'broker1-kr:9092'
TOPICS = ['yellow-taxi', 'fhvhv-taxi']

features = deque(maxlen=500)
kmeans = MiniBatchKMeans(n_clusters=5, random_state=42, batch_size=50)

def extract_features(record, topic):
    try:
        if topic == 'yellow-taxi':
            vec = [
                float(record.get('trip_distance', 0)),
                float(record.get('total_amount', 0)),
                float(record.get('passenger_count', 0))
            ]
        elif topic == 'fhvhv-taxi':
            vec = [
                float(record.get('trip_miles', 0)),
                float(record.get('base_passenger_fare', 0)),
                float(record.get('driver_pay', 0))  # Used instead of passenger count
            ]
        else:
            return None
        return vec
    except Exception as e:
        print("Feature extraction failed:", e)
        return None
def normalize(vec):
    min_vals = [0, 0, 0]
    max_vals = [20, 100, 5]  # Adjust based on domain knowledge
    return [(v - minv) / (maxv - minv + 1e-6) for v, minv, maxv in zip(vec, min_vals, max_vals)]

def process(record, topic):
    vec = extract_features(record, topic)
    if vec is None:
        return
    vec = normalize(vec)  
    print("Raw values:", vec)
    features.append(vec)
    if len(features) >= 50:
        X = np.array(features)
        kmeans.partial_fit(X)
        labels = kmeans.predict(X)
        print("Cluster Centers:\n", kmeans.cluster_centers_)
        print("Sample Labels:", labels[:10])

def create_consumer():
    return Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'stream-clustering',
        'auto.offset.reset': 'earliest'
    })

if __name__ == "__main__":
    consumer = create_consumer()
    consumer.subscribe(TOPICS)
    print(f"Subscribed to topics: {TOPICS}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Kafka error:", msg.error())
                continue
            topic = msg.topic()
            record = json.loads(msg.value().decode('utf-8'))
            process(record, topic)
    except KeyboardInterrupt:
        print("Consumer closed")
    finally:
        consumer.close()
