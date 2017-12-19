#!/usr/bin/env bash

set -e

conda install gcc -y
conda install -c conda-forge fbprophet -y

python3 src/merger.py
python3 src/splitter.py
python3 src/prophet_time_series.py
