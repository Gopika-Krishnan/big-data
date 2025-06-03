#!/bin/bash
#SBATCH --job-name=augment_holweather
#SBATCH --output=augment_holweather_%A_%a.out
#SBATCH --error=augment_holweather_%A_%a.err
#SBATCH --time=01:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=4
#SBATCH --array=0-8

module load Python/3.10.4-GCCcore-11.3.0

pip install --user --quiet --upgrade dask pandas pyarrow tqdm holidays

years=(2016 2017 2018 2019 2020 2021 2022 2023 2024)
year=${years[$SLURM_ARRAY_TASK_ID]}

echo "Running augmentation for year: $year"
python augment_holweather_by_year.py $year
