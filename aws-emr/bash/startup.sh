#!/bin/bash

aws s3 cp ./aws-emr/bash/bootstrap.sh \
    s3://testing-bootstrap-actions/ \
    --profile default

cluster_id=$(aws emr list-clusters \
                 --profile default \
                 --region us-east-1 \
                 --output json | \
                 jq '.Clusters | .[] | select(.Status.State == "WAITING") | .Id' | \
          tr -d '"')

cluster_dns=$(aws emr describe-cluster \
                  --cluster-id $cluster_id \
                  --profile default \
                  --region us-east-1 \
                  --output json | \
                  jq '.Cluster.MasterPublicDnsName' | \
           tr -d '"')

scp -i ./aws-emr/terraform/testing-spark-cluster.pem ./python/* "hadoop@${cluster_dns}":~/.

scp -i ./aws-emr/terraform/testing-spark-cluster.pem ./sandbox/sandbox.cfg "hadoop@${cluster_dns}":~/./aws.cfg

ssh -i ./aws-emr/terraform/testing-spark-cluster.pem "hadoop@${cluster_dns}"

zip my_python_files graphjob.py newsjob.py sparkjob.py tabletest.py

spark-submit --py-files my_python_files.zip etl.py
