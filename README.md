# TWDE Datalab (on AWS)

## Getting started on AWS

We have been exploring different ways to deploy the code on AWS. 
Our first approach was through creating Elastic Map Reduce clusters, but since we settled on pandas instead of Spark at some point, we haven't been doing distributed computing very much.
Therefore, there are two main ways we are using AWS resources: AWS Data Pipeline and Jupyter on EC2. 
We have been using the former to run our decision tree model on larger data sets and the latter (Jupyter on EC2) to run the Prophet time series model.

**IMPORTANT:** The software in the Git repository does not contains AWS credentials or any other way to access an AWS account. 
So, please make sure you have access to an AWS account. 
If you want to use the AWS account of the TWDE Datalab reach out the maintainers.

### Data Pipeline

If you haven't done so, install the AWS command line tools. If you are doing this now, please don't forget to configure your credentials, too.

1. `pip install awscli`
1. ` aws configure` (this will ask you for your credentials and store them in `~/.aws`)

Now run a deployment script from the `deployment` directory

1. `cd deployment`
1. `./deploy-pipeline.sh -j all -n {name for the pipeline goes here}`

This script will do the following:
- create a shell script based on `run_pipeline.sh`
- upload the shell script to S3
- create an AWS data pipeline following `pipeline-definition.json`
- start the pipeline

The output (and logs) are available via the AWS console.
Unfortunately, we've run into some issues with large file sizes, which are documented here https://github.com/ThoughtWorksInc/twde-datalab/issues/25.


### Getting started using Jupyter on EC2

Another, maybe even simpler way to exploit cloud computing, is by [installing Anaconda on AWS EC2 instance](https://hackernoon.com/aws-ec2-part-3-installing-anaconda-on-ec2-linux-ubuntu-dbef0835818a) and [setting up Jupyter Notebooks on AWS](https://towardsdatascience.com/setting-up-and-using-jupyter-notebooks-on-aws-61a9648db6c5). 

For running our Prophet time series model, we published a ready to go AMI image `tw_datalab_prophet_forecast_favorita` that already includes the relevant Jupyter notebooks. 
Just search for this image in 'Community AMIs' when launching an EC2 machine and make sure you open port 8888.
Then ssh into your machine and start the Jupyter server:  

1. `jupyter notebook --no-browser --port=8888`

Afterwards you should be able to open Jupyter in your browser at https://ec2-{public-ip-of-ec2-machine}.{my-region}.compute.amazonaws.com:8888. When asked for a password, simply type 'datalab'.