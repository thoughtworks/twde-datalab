# TWDE-Datalab (on AWS)

## Getting started on AWS

We have also been exploring different ways to deploy the code on AWS. Our first approach was through creating Elastic Map Reduce clusters, but since we haven't been doing distributed computing very much, we're using AWS Data Pipeline. 

**Before you go any further:** The software in the Git repository does not contains AWS credentials or any other way to access an AWS account. So, please make sure you have access to an AWS account

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

At the moment the script ends here. The output (and logs) are available via the AWS console.
