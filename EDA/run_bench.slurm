#!/bin/bash
#SBATCH --job-name=taxi_bench
#SBATCH --output=bench_%j.out
#SBATCH --error=bench_%j.err
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G

module load Python/3.10.4-GCCcore-11.3.0

pip install --user --quiet --upgrade dask pandas pyarrow tqdm holidays geopandas duckdb dask-sql
# source ~/envs/bigdata/bin/activate

srun python benchmark_taxi.py --csv /path/to/sample.csv \
                              --parquet /path/to/sample.parquet \
                              --repeats 5
