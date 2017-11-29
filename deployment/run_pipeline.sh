#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

sudo yum -y update

sudo yum -y install python36 python36-virtualenv python36-pip

sudo python36 -m pip install boto3

aws s3 cp s3://twde-datalab/hello.py .

python36 hello.py