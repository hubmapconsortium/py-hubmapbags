#!/bin/bash

#SBATCH -p batch

module load anaconda
python compute_checksums_on_public_datasets.py
