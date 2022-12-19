#!/bin/bash

#SBATCH -p batch

module load anaconda
python compute_checksums_on_codex_datasets.py
