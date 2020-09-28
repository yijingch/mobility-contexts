#!/bin/bash
#SBATCH --job-name=aggregate_documents
#SBATCH --account=azjacobs1
#SBATCH --partition=standard
#SBATCH --cpus-per-task=12
#SBATCH --mem-per-cpu=8g
#SBATCH --mail-type=NONE

# Load modules
module load python3.6-anaconda

python aggr_per_subm_year_gl.py \
    --start_year=2012 \
    --end_year=2021 
