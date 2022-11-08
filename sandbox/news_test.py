import configparser
import os
import re

from io import BytesIO
from tempfile import SpooledTemporaryFile, TemporaryFile

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import split, col, explode, desc

import boto3
import botocore
import requests

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from bs4 import BeautifulSoup

from htmldate import find_date # may need to (pip uninstall lxml; pip install lxml)

from langdetect import detect

from urllib.parse import urlparse

# from sparknlp.base import *
# from sparknlp.annotator import *
# from sparknlp.pretrained import PretrainedPipeline
# import sparknlp

config = configparser.ConfigParser()
config.read('sandbox.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

spark = SparkSession.builder \
    .config("spark.jars.packages",
            "com.johnsnowlabs.nlp:spark-nlp_2.11:1.8.2") \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
    .getOrCreate()


def process_warcs(_id, iterator):
    for uri in iterator:
        stream = fetch_warc(uri)
        if not stream:
            continue
        try:
            archive_iterator = ArchiveIterator(stream)
            for res in iterate_records(uri, archive_iterator):
                yield res
        except ArchiveLoadFailed as exception:
            print('Invalid WARC: {} - {}'.format(uri, exception))
        finally:
            stream.close()


data_url_pattern = re.compile('^(s3a):(?://([^/]*))?/(.*)')


def fetch_warc(uri):
    uri_match = data_url_pattern.match(uri)
    warctemp = TemporaryFile(mode='w+b')
    stream = None
    if not uri_match:
        warctemp.close()
        return stream
    (scheme, netloc, path) = uri_match.groups()
    try:
        boto3.client('s3').download_fileobj(netloc,
                                            path,
                                            warctemp)
        warctemp.seek(0)
        stream = warctemp
    except botocore.client.ClientError as exception:
        print('Failed to download {}: {}'.format(uri, exception))
        warctemp.close()
    return stream


def iterate_records(_warc_uri, archive_iterator):
    for record in archive_iterator:
        for res in process_record(record):
            yield res


def process_record(record):
    if record.rec_type != 'response':
        # skip over WARC request or metadata records
        yield 'None', 1
    # page = record.content_stream.read()
    uri = record.rec_headers['WARC-Target-URI']
    dom = urlparse(uri).netloc
    yield dom, 1

# crawl-data/CC-NEWS/yyyy/mm/CC-NEWS-yyyymmddHHMMSS-nnnnn.warc.gz
uri = 's3a://commoncrawl/crawl-data/CC-NEWS/2018/06/CC-NEWS-20180614000923-00652.warc.gz'

input_data = spark.sparkContext.parallelize([uri])

output = input_data.mapPartitionsWithIndex(process_warcs) \
    .reduceByKey(lambda a, b: a + b) \
    .take(10) 

# tmp = spark.createDataFrame(output)

print(output)
