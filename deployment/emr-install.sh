#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace
  

pip install --upgrade pip
# Non-standard and non-Amazon Machine Image Python modules:
sudo pip install -U awscli 
sudo pip install -U boto
sudo pip install -U plotly
sudo pip install -U sklearn 
sudo pip install -U seaborn
sudo pip install -U pandas
sudo pip install -U boto
