from confluent_kafka import Producer
import pyarrow.parquet as pq
import pandas as pd
import time
import json
import os

KAFKA_TOPIC = 'taxi-stream'
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
            file_path = os.path.join(directory, file)
            try:
                # Read row groups one at a time to save memory
                parquet_file = pq.ParquetFile(file_path)
                for rg in range(parquet_file.num_row_groups):
                    table = parquet_file.read_row_group(rg)
                    df = table.to_pandas()
                    df = df.sort_values('tpep_pickup_datetime')

                    for _, row in df.iterrows():
                        record = row.to_dict()
                        for k, v in record.items():
                            if isinstance(v, pd.Timestamp):
                                record[k] = v.isoformat()

                        location_key = 'pulocationid'
                        key = str(record.get(location_key, 'Unknown'))

                        producer.produce(KAFKA_TOPIC, json.dumps(record), key=key, callback=delivery_report)
                        producer.poll(0)
                        time.sleep(0.01)  # Simulate stream
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    producer = create_producer()
    stream_parquet('/workspace/yellow_partitioned/year=2021', producer)
    producer.flush()
