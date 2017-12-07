#!/usr/bin/env bash


while getopts 'j:n:' arg
do
  case ${arg} in
    j) job=${OPTARG};;
    n) name=${OPTARG};;
    *) echo "Unexpected argument"
  esac
done

if [ -z "$job" ] ; then
    echo 'Usage: deploy_pipeline.sh -j <merger | splitter | decision_tree | all> [-n <pipeline name>]'
    exit 1
fi


cp run_pipeline.sh run_pipeline_job.sh

if [ "$job" = "all" ]; then
	echo "python36 merger.py" >> run_pipeline_job.sh
	echo "python36 splitter.py" >> run_pipeline_job.sh
	echo "python36 decision_tree.py" >> run_pipeline_job.sh
else 
  echo "python36 $job.py" >> run_pipeline_job.sh
fi

echo "Uploading run_pipeline_job.sh for $job"
aws s3 cp run_pipeline_job.sh s3://twde-datalab/

echo "Uploading src/ to twde-datalab/src.tar.gz"
rm ../src/*.hdf
tar czf src.tar.gz --directory="../src/" .
aws s3 cp "src.tar.gz" "s3://twde-datalab/src.tar.gz"

response=`aws datapipeline create-pipeline --name $name-$(gdate -u +%FT%T) --unique-id datalab-pipeline-$(gdate -u +%FT%T)`

id=$(echo $response | jq --raw-output '.pipelineId')

echo "Putting pipeline-definition for pipeline $id"
aws datapipeline put-pipeline-definition --pipeline-id $id --pipeline-definition file://pipeline-definition.json

echo "Activating pipeline"
aws datapipeline activate-pipeline --pipeline-id $id

#aws datapipeline deactivate-pipeline --pipeline-id $id --no-cancel-active
