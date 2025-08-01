#!/bin/bash
#SBATCH --job-name=task3_green2024
#SBATCH --partition=cpu
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=01:00:00
#SBATCH --output=/d/hpc/projects/FRI/ma76193/logs/task3_green2024_%j.out
#SBATCH --error=/d/hpc/projects/FRI/ma76193/logs/task3_green2024_%j.err
#SBATCH --mail-type=END
#SBATCH --mail-user=ma76193@student.uni-lj.si

# === MODULE SETUP ===
module purge
module load Python/3.11.5-GCCcore-13.2.0

# Ensure the log directory exists
mkdir -p /d/hpc/projects/FRI/ma76193/logs

echo "===== TASK 3 JOB START ====="
echo " Job ID    : $SLURM_JOB_ID"
echo " Partition : $SLURM_JOB_PARTITION"
echo " Nodes     : $SLURM_JOB_NODELIST"
echo " CPUs      : $SLURM_CPUS_PER_TASK"
echo " Mem       : $SLURM_MEM_PER_NODE"
echo " Submit Dir: $(pwd)"
echo "==========================="

# Switch into the directory where t3_conversion.py lives
cd /d/hpc/projects/FRI/ma76193/big-data

# Run the Python script
python t3_conversion.py

EXIT_CODE=$?
echo "===== TASK 3 JOB END (exit=$EXIT_CODE) at $(date) ====="
exit $EXIT_CODE
