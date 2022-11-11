#!/bin/bash

aws s3 cp ./aws-emr/bash/bootstrap.sh \
    s3://capstone-emr-boostrap-action/ \
    --profile udac_de_cap

cluster_id=$(aws emr list-clusters --profile udac_de_cap --region us-east-1 | \
                 jq '.Clusters | .[] | select(.Status.State == "WAITING") | .Id' | \
          tr -d '"')

cluster_dns=$(aws emr describe-cluster \
                  --cluster-id $cluster_id --profile udac_de_cap --region us-east-1 | \
                  jq '.Cluster.MasterPublicDnsName' | \
           tr -d '"')

scp -i ./aws-emr/terraform/spark-cluster.pem ./sandbox/news_test.py "hadoop@${cluster_dns}":~/.

scp -i ./aws-emr/terraform/spark-cluster.pem ./sandbox/sandbox.cfg "hadoop@${cluster_dns}":~/.

ssh -i ./aws-emr/terraform/spark-cluster.pem "hadoop@${cluster_dns}"
