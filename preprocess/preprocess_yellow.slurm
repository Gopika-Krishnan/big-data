#!/bin/bash
#SBATCH --job-name=preprocess_yellow
#SBATCH --output=preprocess_yellow.out
#SBATCH --error=preprocess_yellow.err
#SBATCH --time=01:00:00
#SBATCH --mem=128G
#SBATCH --cpus-per-task=16

module load Python/3.10.4-GCCcore-11.3.0

pip install --user --quiet --upgrade dask pandas pyarrow tqdm

python preprocess_yellow.py
