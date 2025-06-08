import faust
from statistics import mean, stdev
import csv
import datetime
import logging
from dateutil import parser
import numpy as np

# ========== Logging Setup ==========

log = logging.getLogger(__name__)
log.setLevel(logging.WARNING)

formatter = logging.Formatter('[%(asctime)s] [%(process)d] [%(levelname)s] %(message)s')

file_handler = logging.FileHandler('faust_stats.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.WARNING)

log.addHandler(file_handler)


# ========== Faust Record ==========
from datetime import datetime

class TaxiRecord(faust.Record, serializer='json'):
    pulocationid: int
    trip_distance: float
    fare_amount: float
    pickup_datetime: datetime
    dropoff_datetime: datetime


# ========== Faust App Setup ==========
app = faust.App(
    'yellow-taxi-stats',
    broker='kafka://broker1-kr:9092',
    value_serializer='json',
)

topic = app.topic('yellow-taxi', value_type=TaxiRecord)

WINDOW_SIZE = 100

location_stats = app.Table('location_stats', default=list, partitions=1)
borough_stats = app.Table('borough_stats', default=list, partitions=1)
pickup_counts = app.Table('pickup_counts', default=int, partitions=1)

location_to_borough = {}

@app.task
async def load_lookup():
    with open('zone_lookup.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            location_id = int(row['LocationID'])
            borough = row['Borough']
            location_to_borough[location_id] = borough
    log.warning(f"Loaded {len(location_to_borough)} zone mappings.")

@app.agent(topic)
async def process(records):
    async for record in records:
        loc_id = str(record.pulocationid)
        borough = location_to_borough.get(record.pulocationid, 'Unknown')

        try:
            pickup_time = parser.isoparse(record.pickup_datetime)
            dropoff_time = parser.isoparse(record.dropoff_datetime)
            duration_min = (dropoff_time - pickup_time).total_seconds() / 60.0
        except Exception as e:
            log.warning(f"Error parsing datetime for record {record}: {e}")
            continue

        entry = (record.trip_distance, record.fare_amount, duration_min)

        location_stats[loc_id].append(entry)
        if len(location_stats[loc_id]) > WINDOW_SIZE:
            location_stats[loc_id].pop(0)

        borough_entries = list(borough_stats.get(borough, []))
        borough_entries.append(entry)
        if len(borough_entries) > WINDOW_SIZE:
            borough_entries.pop(0)
        borough_stats[borough] = borough_entries

        pickup_counts[loc_id] += 1

NUM_CLUSTERS = 3
cluster_centers = np.random.rand(NUM_CLUSTERS, 3)  # Init randomly
def normalize(vec):
    return [
        vec[0] / 10.0,    # distance (assume typical max ~10 km)
        vec[1] / 50.0,    # fare (assume typical max ~50 USD)
        vec[2] / 60.0     # duration (assume typical max ~60 minutes)
    ]
cluster_counts = np.zeros(NUM_CLUSTERS)
def update_clusters(features_dict):
    global cluster_centers, cluster_counts
    for loc_id, vec in features_dict.items():
        point = np.array(vec)
        dists = np.linalg.norm(cluster_centers - point, axis=1)
        cluster_idx = np.argmin(dists)

        # Increment count and update cluster center using cumulative average
        cluster_counts[cluster_idx] += 1
        eta = 1.0 / cluster_counts[cluster_idx]
        cluster_centers[cluster_idx] = (1 - eta) * cluster_centers[cluster_idx] + eta * point

        log.warning(f"Location {loc_id} assigned to cluster {cluster_idx}")


@app.timer(interval=30.0)
async def print_top_locations():
    top_locations = sorted(pickup_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    features_dict = {}
    log.warning("=== Top 10 Pickup Locations ===")
    for loc_id, count in top_locations:
        borough = location_to_borough.get(int(loc_id), "Unknown")
        entries = location_stats.get(loc_id, [])
        if not entries:
            continue
        try:
            valid_entries = [x for x in entries if isinstance(x, tuple) and len(x) == 3]
            if not valid_entries:
                continue
            distances = [x[0] for x in valid_entries]
            fares = [x[1] for x in valid_entries]
            durations = [x[2] for x in valid_entries]
            # features_dict[loc_id] = [mean(distances), mean(fares), mean(durations)]
            features_dict[loc_id] = normalize([mean(distances), mean(fares), mean(durations)])

            
            log.warning(f"Location {loc_id} ({borough}) - Pickups: {count}")
            log.warning(f"  Trips: {len(entries)} | Mean Distance: {mean(distances):.2f} | Std Distance: {stdev(distances) if len(distances) > 1 else 0.0:.2f}")
            log.warning(f"  Mean Fare: {mean(fares):.2f} | Std Fare: {stdev(fares) if len(fares) > 1 else 0.0:.2f}")
            log.warning(f"  Mean Duration: {mean(durations):.2f} mins | Std Duration: {stdev(durations) if len(durations) > 1 else 0.0:.2f}")
        except Exception as e:
            log.warning(f"Error processing location {loc_id}: {e}")
    update_clusters(features_dict)  # Add this line at the end


@app.timer(interval=30.0)
async def print_borough_stats():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.warning(f"=== Borough Stats at {now} ===")
    for borough, entries in borough_stats.items():
        if not entries:
            continue
        try:
            distances = [x[0] for x in entries]
            fares = [x[1] for x in entries]
            durations = [x[2] for x in entries]
            log.warning(f"Borough: {borough}")
            log.warning(f"  Trips: {len(entries)} | Mean Distance: {mean(distances):.2f} | Std Distance: {stdev(distances) if len(distances) > 1 else 0.0:.2f}")
            log.warning(f"  Mean Fare: {mean(fares):.2f} | Std Fare: {stdev(fares) if len(fares) > 1 else 0.0:.2f}")
            log.warning(f"  Mean Duration: {mean(durations):.2f} mins | Std Duration: {stdev(durations) if len(durations) > 1 else 0.0:.2f}")
        except Exception as e:
            log.warning(f"Error processing borough {borough}: {e}")
