#!/bin/bash
#SBATCH --job-name=export_csv
#SBATCH --output=export_csv.out
#SBATCH --error=export_csv.err
#SBATCH --time=02:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=4
#SBATCH --partition=all

module load Python/3.10.4-GCCcore-11.3.0

pip install --user --quiet --upgrade dask pandas pyarrow tqdm

python export_csv_splits.py
