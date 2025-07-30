#!/bin/bash
#SBATCH --job-name=task8_fhvhv_analysis
#SBATCH --output=task8.out
#SBATCH --error=task8.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=01:30:00

module load Python/3.10.4-GCCcore-11.3.0
python -m venv $TMPDIR/env
source $TMPDIR/env/bin/activate
pip install --quiet --upgrade pip
pip install --quiet duckdb dask[complete] pyarrow pandas matplotlib 

sleep 60

python task8.py