#!/bin/bash
#SBATCH --job-name=augment_holweather
#SBATCH --output=augment_holweather_%A_%a.out
#SBATCH --error=augment_holweather_%A_%a.err
#SBATCH --time=06:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=2
#SBATCH --array=0-34%4

module load Python/3.10.4-GCCcore-11.3.0

# Set up a clean virtual environment in the temp dir
python -m venv $TMPDIR/env
source $TMPDIR/env/bin/activate

# Install dependencies
pip install --upgrade pip
pip install pyarrow tqdm holidays pandas dask fastparquet

# Define all valid (dataset, year) combinations
combinations=(
    "yellow 2012" "yellow 2013" "yellow 2014" "yellow 2015" "yellow 2016" "yellow 2017" "yellow 2018" "yellow 2019" 
    "yellow 2020" "yellow 2021" "yellow 2022" "yellow 2023" "yellow 2024" 
    "green 2016" "green 2017" "green 2018" "green 2019" "green 2020" "green 2021" "green 2022"
    "green 2023" "green 2024"
    "fhv 2016" "fhv 2017" "fhv 2018" "fhv 2019" "fhv 2020" "fhv 2021" "fhv 2022" "fhv 2023" "fhv 2024"
    "fhvhv 2019" "fhvhv 2022" "fhvhv 2023" "fhvhv 2024"
)

combo="${combinations[$SLURM_ARRAY_TASK_ID]}"
read dataset year <<< "$combo"

echo "🚕 Running hol+weather augmentation for dataset: $dataset, year: $year"

python augment_holweather.py "$dataset" "$year"
