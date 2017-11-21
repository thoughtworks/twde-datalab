#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

sudo yum -y update
sudo easy_install-3.4 pip

sudo python3 -m pip install pandas
sudo python3 -m pip install awscli
sudo python3 -m pip install plotly
sudo python3 -m pip install sklearn
sudo python3 -m pip install seaborn
sudo python3 -m pip install boto3

