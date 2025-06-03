#!/bin/bash
#SBATCH --job-name=augment_POI
#SBATCH --output=augment_POI.out
#SBATCH --error=augment_POI.err
#SBATCH --time=02:00:00
#SBATCH --mem=100G
#SBATCH --reservation=fri
#SBATCH --cpus-per-task=16

module load Python/3.10.4-GCCcore-11.3.0

pip install --user --quiet --upgrade dask pandas pyarrow tqdm holidays geopandas

echo "Running augmentation of POIs: "
python augment_POI.py