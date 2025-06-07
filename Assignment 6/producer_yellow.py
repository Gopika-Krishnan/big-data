# producer_yellow_taxi.py

from confluent_kafka import Producer
import pyarrow.parquet as pq
import pandas as pd
import time
import json
import os

TOPIC = 'yellow-taxi'
BOOTSTRAP_SERVERS = 'broker1-kr:9092'

def create_producer():
    return Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered: {msg.topic()} [{msg.partition()}]")

def stream_parquet(directory, producer):
    files = sorted(os.listdir(directory))
    for file in files:
        if file.endswith('.parquet'):
            parquet_file = pq.ParquetFile(os.path.join(directory, file))
            for rg in range(parquet_file.num_row_groups):
                table = parquet_file.read_row_group(rg)
                df = table.to_pandas().sort_values('tpep_pickup_datetime')
                for _, row in df.iterrows():
                    try:
                        record = {
                            'pulocationid': int(row['pulocationid']),
                            'trip_distance': float(row['trip_distance']),
                            'fare_amount': float(row['fare_amount']),
                            'pickup_datetime': row['tpep_pickup_datetime'].isoformat(),
                            'dropoff_datetime': row['tpep_dropoff_datetime'].isoformat()
                        }
                        key = str(record['pulocationid'])
                        print(f"Sending record: {record}")
                        producer.produce(
                            TOPIC,
                            json.dumps(record),
                            key=key,
                            callback=delivery_report
                        )
                        producer.poll(0)
                        time.sleep(0.01)
                    except Exception as e:
                        print(f"Skipping row due to error: {e}")

if __name__ == "__main__":
    p = create_producer()
    stream_parquet('/workspace/yellow_partitioned/year=2021', p)
    p.flush()
