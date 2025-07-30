#!/bin/bash
#SBATCH --job-name=augment_POI
#SBATCH --output=augment_POI_%j.out
#SBATCH --error=augment_POI_%j.err
#SBATCH --time=06:00:00
#SBATCH --mem=200G
#SBATCH --cpus-per-task=4

module purge
module load Python/3.10.4-GCCcore-11.3.0

# Set up virtual environment in scratch (faster)
python -m venv $TMPDIR/env
source $TMPDIR/env/bin/activate

# Install dependencies in memory
pip install --no-cache-dir --upgrade pip
pip install --no-cache-dir pandas geopandas pyarrow dask fastparquet fsspec shapely

# Run the Python script
python /d/hpc/projects/FRI/jo83525/big-data/augment/augment_POI.py
