#!/bin/bash
#SBATCH --job-name=dec22to26_extract
#SBATCH --output=extract_out.log
#SBATCH --error=extract_err.log
#SBATCH --time=01:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=2

module load Python/3.10.4-GCCcore-11.3.0

pip install --user --quiet --upgrade dask pandas pyarrow tqdm
pythonmake_subset.py
