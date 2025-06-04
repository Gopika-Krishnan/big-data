from confluent_kafka import Consumer
import json
import pandas as pd
from collections import defaultdict, deque
from statistics import mean, stdev

TOPICS = ['yellow-taxi', 'fhvhv-taxi']
BOOTSTRAP_SERVERS = 'broker1-kr:9092'

def create_consumer():
    return Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'rolling-stats',
        'auto.offset.reset': 'earliest'
    })
zone_df = pd.read_csv('zone_lookup.csv')
location_to_borough = dict(zip(zone_df['LocationID'], zone_df['Borough']))

ROLLING_SIZE = 100
attributes = ['trip_distance', 'passenger_count', 'total_amount']

windows = defaultdict(lambda: defaultdict(lambda: deque(maxlen=ROLLING_SIZE)))

def process(record):
    try:
        loc_id = int(record.get('pulocationid', -1))
        borough = location_to_borough.get(loc_id, 'Unknown')
    except:
        borough = 'Unknown'

    for attr in attributes:
        val = float(record.get(attr, 0.0) or 0.0)
        windows[borough][attr].append(val)

    output = {'borough': borough}
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
            if msg is None: continue
            if msg.error(): continue
            record = json.loads(msg.value().decode('utf-8'))
            result = process(record)
            print(result)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
