#!/bin/bash


# Defining environment variables
export BUCKET_NAME="pubmed-ftp-clone"
export TXT_FILE_PATH="PMC1043859.txt"
export PROJECT_NAME="psyched-garage-393419"
export OPENIE6_PYTHON_LOC="/opt/conda/envs/openie6/bin/python"


cat $PROJECT_NAME
python analyze_paper.py 