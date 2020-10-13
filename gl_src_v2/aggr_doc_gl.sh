#!/bin/bash
#SBATCH --job-name=aggregate_documents
#SBATCH --account=azjacobs1
#SBATCH --partition=largemem
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=100g
#SBATCH --time=8:00:00
#SBATCH --mail-type=NONE

# Load modules
module load python3.6-anaconda

python aggr_doc_year_gl.py \
      --start_year=2012 \
      --end_year=2014
