#!/bin/bash

#SBATCH -p batch

module load anaconda
python ./generate_uuid_report.py
