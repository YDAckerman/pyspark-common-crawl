#!/bin/bash

# FIRST:
# setup s3 bucket and create the cluster key-pair

# move the bootstrap file to s3

aws s3 cp ./aws-emr/bash/bootstrap.sh \
    s3://testing-bootstrap-actions/ \
    --profile default

# provision the EMR cluster

cd ./aws-emr/terraform
terraform init
terraform validate
terraform apply
cd ../../

# get cluster information

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

# move the python and configuration files to the cluster master node

scp -i ./aws-emr/terraform/testing-spark-cluster.pem ./python/* "hadoop@${cluster_dns}":~/.

scp -i ./aws-emr/terraform/testing-spark-cluster.pem ./sandbox/sandbox.cfg "hadoop@${cluster_dns}":~/./aws.cfg

# ssh into the cluster, zip the files, and run the etl script

ssh -i ./aws-emr/terraform/testing-spark-cluster.pem "hadoop@${cluster_dns}"

zip my_python_files graphjob.py newsjob.py sparkjob.py tabletest.py

spark-submit --py-files my_python_files.zip etl.py
