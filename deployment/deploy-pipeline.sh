#!/usr/bin/env bash

if [[ $# -eq 0 ]] ; then
    echo 'Usage: deploy_pipeline.sh -j <job name>'
    exit 1
fi

while getopts 'j:' arg
do
  case ${arg} in
    j) job=${OPTARG};;
    *) echo "Unexpected argument"
  esac
done

sed -e "s/%%JOBNAME%%/$job/g" run_pipeline.sh > run_pipeline_job.sh

echo "Uploading run_pipeline_job.sh for $job"
aws s3 cp run_pipeline_job.sh s3://twde-datalab/

echo "Uploading src/ to twde-datalab/src.tar.gz"
tar czf src.tar.gz --directory="../src/" .
aws s3 cp "src.tar.gz" "s3://twde-datalab/src.tar.gz"

response=`aws datapipeline create-pipeline --name datalab-pipeline-$(gdate -u +%FT%T) --unique-id datalab-pipeline-$(gdate -u +%FT%T)`

id=$(echo $response | jq --raw-output '.pipelineId')

echo "Putting pipeline-definition for pipeline $id"
aws datapipeline put-pipeline-definition --pipeline-id $id --pipeline-definition file://pipeline-definition.json

echo "Activating pipeline"
aws datapipeline activate-pipeline --pipeline-id $id

#TODO sleep X
#aws datapipeline deactivate-pipeline --pipeline-id $id --no-cancel-active
