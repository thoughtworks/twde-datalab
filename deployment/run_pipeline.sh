#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

sudo yum -y update

sudo yum -y install python36 python36-virtualenv python36-pip

sudo python36 -m pip install boto3
sudo python36 -m pip install pandas
sudo python36 -m pip install s3fs
# sudo python36 -m pip install numpy
sudo python36 -m pip install joblib
sudo python36 -m pip install sklearn
sudo python36 -m pip install scipy
sudo python36 -m pip install tables
# sudo python36 -m pip install jupyter

mkdir -p src && cd src
aws s3 cp s3://twde-datalab/src.tar.gz .
tar xzf src.tar.gz

# Jobs to be executed will be appeneded below
