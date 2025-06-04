from confluent_kafka import Consumer
import json
import pandas as pd
from collections import defaultdict, deque, Counter
from statistics import mean, stdev

# Kafka configuration
TOPICS = ['yellow-taxi', 'fhvhv-taxi']
BOOTSTRAP_SERVERS = 'broker1-kr:9092'
ROLLING_SIZE = 100
ATTRIBUTES = ['trip_distance', 'passenger_count', 'total_amount']

# Load zone lookup table for mapping location IDs to boroughs
zone_df = pd.read_csv('zone_lookup.csv')
location_to_borough = dict(zip(zone_df['LocationID'], zone_df['Borough']))

# Track top pickup/dropoff locations
pickup_counter = Counter()
dropoff_counter = Counter()

# Rolling windows for stats per group (e.g., borough or pickup location)
windows = defaultdict(lambda: defaultdict(lambda: deque(maxlen=ROLLING_SIZE)))

def create_consumer():
    return Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'rolling-stats',
        'auto.offset.reset': 'earliest'
    })

def process(record):
    pickup = str(record.get('pulocationid', 'Unknown'))
    dropoff = str(record.get('dolocationid', 'Unknown'))

    pickup_counter[pickup] += 1
    dropoff_counter[dropoff] += 1

    # Group by borough if known, else fallback to pickup ID
    pickup_id = record.get('pulocationid')
    borough = location_to_borough.get(pickup_id, str(pickup_id))
    group = borough


    stats = {}
    for attr in ATTRIBUTES:
        val = record.get(attr)
        try:
            val = float(val)
        except (ValueError, TypeError):
            continue

        windows[group][attr].append(val)
        values = list(windows[group][attr])
        stats[attr] = {
            'mean': mean(values),
            'std': stdev(values) if len(values) > 1 else 0.0,
            'count': len(values)
        }

    # Every 1000 messages, show top pickup/dropoff locations
    if sum(pickup_counter.values()) % 1000 == 0:
        print("\nTop 10 Pickups:")
        for loc_id, count in pickup_counter.most_common(10):
            borough = location_to_borough.get(int(loc_id), 'Unknown') if loc_id.isdigit() else 'Unknown'
            print(f"   {loc_id} ({borough}): {count} pickups")

        print("Top 10 Dropoffs:")
        for loc_id, count in dropoff_counter.most_common(10):
            borough = location_to_borough.get(int(loc_id), 'Unknown') if loc_id.isdigit() else 'Unknown'
            print(f"   {loc_id} ({borough}): {count} dropoffs")

    return {'group': group, 'stats': stats}

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

            try:
                record = json.loads(msg.value().decode('utf-8'))
                result = process(record)
                print(result)
            except Exception as e:
                print(f"Error processing message: {e}")
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()
