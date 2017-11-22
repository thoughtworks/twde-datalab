#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

sudo yum -y update
sudo easy_install-3.4 pip
wget https://github.com/pypa/pip/archive/9.0.1.zip
unzip 9.0.1.zip
cd pip-9.0.1/
sudo python3 setup.py install

sudo python3 -m pip install pandas
#sudo python3 -m pip install awscli
sudo python3 -m pip install plotly
#sudo python3 -m pip install sklearn 
sudo python3 -m pip install seaborn
sudo python3 -m pip install boto3
sudo python3 -m pip install pyspark
sudo python3 -m pip install pyarrow
sudo python3 -m pip install s3fs

