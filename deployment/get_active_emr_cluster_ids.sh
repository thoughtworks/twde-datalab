aws emr list-clusters --active | jq '.Clusters [] .Id'
