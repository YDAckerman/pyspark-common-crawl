#!/bin/bash

# copied nearly verbatim from https://nlp.johnsnowlabs.com/docs/en/install
# with the addition of a few python packages

set -x -e

echo -e 'export PYSPARK_PYTHON=/usr/bin/python3
export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_JARS_DIR=/usr/lib/spark/jars
export SPARK_HOME=/usr/lib/spark' >> $HOME/.bashrc && source $HOME/.bashrc

sudo python3 -m pip install \
     warcio \
     boto3 \
     botocore \
     requests \
     ujson \
     idna \
     lxml \
     beautifulsoup4 \
     htmldate \
     spark-nlp

set +x
exit 0
