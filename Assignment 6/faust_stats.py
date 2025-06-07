# faust_app.py
import pandas as pd

import faust
from statistics import mean, stdev
from collections import deque, defaultdict

# Define Faust app
app = faust.App(
    'yellow-taxi-stream',
    broker='kafka://localhost:10002',
    value_serializer='json',
)

# Define input Kafka topic
yellow_topic = app.topic('yellow-taxi')

# Read zone lookup
zone_df = pd.read_csv('zone_lookup.csv')
location_to_borough = dict(zip(zone_df['LocationID'], zone_df['Borough']))

# Set up data structures
ROLLING_SIZE = 100
attributes = ['trip_distance', 'fare_amount', 'passenger_count']
windows = defaultdict(lambda: defaultdict(lambda: deque(maxlen=ROLLING_SIZE)))

@app.agent(yellow_topic)
async def process(records):
    async for record in records:
        try:
            loc_id = int(record.get('pulocationid', -1))
            borough = location_to_borough.get(loc_id, 'Unknown')
        except:
            borough = 'Unknown'

        result = {'borough': borough}
        for attr in attributes:
            val = float(record.get(attr, 0.0) or 0.0)
            windows[borough][attr].append(val)
            values = list(windows[borough][attr])
            result[f"{attr}_mean"] = round(mean(values), 3)
            result[f"{attr}_std"] = round(stdev(values), 3) if len(values) > 1 else 0.0
            result[f"{attr}_count"] = len(values)

        print(f"Processed rolling stats: {result}")
