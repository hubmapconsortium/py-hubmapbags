#!/bin/bash

#SBATCH -p batch

module load anaconda
python ./create_report_on_public_datasets.py
