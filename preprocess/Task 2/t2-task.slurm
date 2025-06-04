#!/bin/bash
#SBATCH --job-name=t2yellow
#SBATCH --output=t2yellow.out
#SBATCH --error=t2yellow.err
#SBATCH --time=02:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=2
#SBATCH --partition=mig-priority

set -euo pipefail

echo "Job started at: $(date)"

# Load required module
module load Python/3.10.4-GCCcore-11.3.0

# Create and activate virtual environment in temporary directory
python -m venv $TMPDIR/env
source $TMPDIR/env/bin/activate

# Install necessary packages
pip install --upgrade pip
pip install dask[complete] pandas pyarrow matplotlib seaborn

# Run the script
srun python t2-yellow.py

echo "Job finished at: $(date)"
