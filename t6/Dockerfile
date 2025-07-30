FROM python:3.7-slim

# Install system deps including librdkafka for confluent-kafka
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc g++ make \
       librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip --timeout 300

# Install Python dependencies
RUN pip install numpy pandas pyarrow confluent-kafka faust scikit-multiflow matplotlib