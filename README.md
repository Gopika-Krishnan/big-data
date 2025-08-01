# NYC TLC Big Data Project

**Authors**: Gopika Krishnan, Juan Osorio, Muhammad Ali

---

## Project Overview

This repository contains our end-to-end “Big Data” analysis of New York City’s Taxi & Limousine Commission (TLC) Trip Record Data, We cover:

1. **Data Preparation**  
   - Year-based Parquet partitioning (Task 1)  
   - Data quality profiling and cleaning (Task 2)  
2. **Format Benchmarking**  
   - CSV, gzipped CSV, HDF5, DuckDB conversion & I/O timing (Task 3)  
3. **Exploratory Data Analysis**  
   - Temporal & spatial aggregates  
   - Cross-dataset similarity (Task 4)  
4. **Augmentation & Streaming**  
   - POI, holiday & weather augmentation (Task 5)  
   - Kafka producer/consumer streaming & clustering (Task 6)  
5. **Machine Learning & Influence Analysis**  
   - Distributed demand forecasting (Task 7)  
   - Impact of app-based services on traditional taxis (Task 8)
6. **Map Visualizations**  
   - Geospatial overlays & network graphs (Task 9)


We leverage **Python** with **Dask**, **DuckDB**, **PyArrow**, **Pandas**, **SLURM**, **Kafka**, **LightGBM/XGBoost**, and **GeoPandas** on the ARNES HPC cluster.
