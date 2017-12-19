#!/usr/bin/env bash

set -e

python3 src/merger.py
python3 src/splitter.py
python3 src/prophet_time_series.py
