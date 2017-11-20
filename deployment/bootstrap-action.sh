#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

sudo yum -y update
sudo easy_install-3.4 pip

python3 -m pip install --user pandas
python3 -m pip install --user awscli
python3 -m pip install --user plotly
python3 -m pip install --user sklearn 
python3 -m pip install --user seaborn

