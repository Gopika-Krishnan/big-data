#!/bin/bash
#SBATCH --job-name=scaling_test_citywide
#SBATCH --output=logs/%x_%A_%a.out   # Path for standard output
#SBATCH --error=logs/%x_%A_%a.err    # Path for error output
#SBATCH --nodes=1                     # Number of nodes
#SBATCH --ntasks=1                    # Number of tasks (processes)
#SBATCH --cpus-per-task=16            # CPU cores per task
#SBATCH --mem=64G                     # Memory per node
#SBATCH --time=02:00:00               # Walltime limit
#SBATCH --array=0-2                   # Array indices: test 3 different worker counts

# Load required Python module and activate virtual environment
module load Python/3.11.3-GCCcore-12.3.0
source ~/citywide-venv/bin/activate

# Change to submission directory
cd "$SLURM_SUBMIT_DIR"

# Worker configuration arrays indexed by SLURM_ARRAY_TASK_ID
WORKERS=(1 2 4)                       # Number of Dask workers to launch
THREADS=(4 4 4)                       # Threads per worker
MEMS=(8GB 8GB 8GB)                   # Memory limit per worker

# Select configuration for this job
NUM_WORKERS=${WORKERS[$SLURM_ARRAY_TASK_ID]}
THREADS_PER_WORKER=${THREADS[$SLURM_ARRAY_TASK_ID]}
MEMORY_PER_WORKER=${MEMS[$SLURM_ARRAY_TASK_ID]}

# Define paths and filenames for Dask scheduler and output
BASE_PATH="/d/hpc/projects/FRI/jo83525/big-data/T5/POI"
SCHED_FILE="scheduler_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}.json"
OUT_CSV="results_scaling_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}.csv"

# Launch Dask scheduler in the background
echo "Starting scheduler with ${NUM_WORKERS} workers, memory ${MEMORY_PER_WORKER} per worker..."
dask-scheduler --scheduler-file "$SCHED_FILE" > logs/scheduler_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}.log 2>&1 &

# Give the scheduler time to initialize
sleep 5

# Launch each Dask worker
for i in $(seq 1 $NUM_WORKERS); do
    dask-worker \
        --scheduler-file "$SCHED_FILE" \
        --nthreads "$THREADS_PER_WORKER" \
        --memory-limit "$MEMORY_PER_WORKER" \
        > logs/worker_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}_$i.log 2>&1 &
done

# Allow workers to connect
sleep 10

# Run the training script with the configured scheduler
echo "Running training job with $NUM_WORKERS workers..."
python mvp_citywide.py \
    -b "$BASE_PATH" \
    -s "$SCHED_FILE" \
    --sample-n None \
    -o "$OUT_CSV"

# Indicate completion and output location
echo "Training complete. Results saved to $OUT_CSV"
