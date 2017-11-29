#!/usr/bin/env bash

response=`aws datapipeline create-pipeline --name datalab-pipeline-$(gdate -u +%FT%T) --unique-id datalab-pipeline-$(gdate -u +%FT%T)`

id=$(echo $response | jq --raw-output '.pipelineId')

aws datapipeline put-pipeline-definition --pipeline-id $id --pipeline-definition file://pipeline-definition.json

aws datapipeline activate-pipeline --pipeline-id $id