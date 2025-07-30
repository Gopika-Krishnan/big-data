#!/bin/bash
#SBATCH --job-name=preprocess_all
#SBATCH --output=preprocess_all.out
#SBATCH --error=preprocess_all.err
#SBATCH --time=02:00:00
#SBATCH --mem=100G
#SBATCH --cpus-per-task=2
set -euo pipefail

echo "Job started at: $(date)"

# Load Python module
module load Python/3.10.4-GCCcore-11.3.0

# Set up a clean virtual environment in the temp dir
python -m venv $TMPDIR/env
source $TMPDIR/env/bin/activate

# Install dependencies
pip install --upgrade pip
pip install pyarrow tqdm pandas dask

# Run your preprocessing script
srun python preprocess_all.py

echo "Job finished at: $(date)"
