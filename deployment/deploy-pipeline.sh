#!/usr/bin/env bash

while getopts 'j:' arg
do
  case ${arg} in
    j) job=${OPTARG};;
    *) echo "Unexpected argument"
  esac
done
echo

sed -e "s/%%JOBNAME%%/$job/g" run_pipeline.sh > run_pipeline_job.sh

aws s3 cp run_pipeline_job.sh s3://twde-datalab/

response=`aws datapipeline create-pipeline --name datalab-pipeline-$(gdate -u +%FT%T) --unique-id datalab-pipeline-$(gdate -u +%FT%T)`

id=$(echo $response | jq --raw-output '.pipelineId')

aws datapipeline put-pipeline-definition --pipeline-id $id --pipeline-definition file://pipeline-definition.json

aws datapipeline activate-pipeline --pipeline-id $id

#TODO sleep X
#aws datapipeline deactivate-pipeline --pipeline-id $id --no-cancel-active