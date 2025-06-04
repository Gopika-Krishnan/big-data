# rest same, but stream_parquet('/workspace/fhvhv_partitioned/year=2021', p)
# Stream Yellow Taxi data into "yellow-taxi" topic
from confluent_kafka import Producer
import pyarrow.parquet as pq
import pandas as pd
import time
import json
import os

TOPIC = 'fhvhv-taxi'
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
                df = table.to_pandas().sort_values('pickup_datetime')
                for _, row in df.iterrows():
                    record = row.to_dict()
                    for k, v in record.items():
                        if isinstance(v, pd.Timestamp):
                            record[k] = v.isoformat()
                        elif isinstance(v, (float, int, str, type(None))):
                            record[k] = v
                        else:
                            try:
                                record[k] = v.item()  # for np.int64, np.float32, etc.
                            except:
                                record[k] = str(v)    # fallback: stringify unknowns

                    key = str(record.get('pulocationid', 'Unknown'))
                    producer.produce(TOPIC, json.dumps(record), key=key, callback=delivery_report)
                    producer.poll(0)
                    time.sleep(0.01)

if __name__ == "__main__":
    p = create_producer()
    stream_parquet('/workspace/fhvhv_partitioned/year=2021', p)
    p.flush()
