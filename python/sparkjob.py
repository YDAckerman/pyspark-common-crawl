
import re
import logging

from tempfile import TemporaryFile

from pyspark.sql import SparkSession

import boto3
import botocore

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

import configparser

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


class MySparkJob():

    name = 'SparkJob'
    log_level = 'INFO'
    s3client = None

    def __init__(self, s3_bucket, warc_gz_path, cfg_path,
                 log_level=None, local_test=False):

        self.s3_bucket = s3_bucket
        self.warc_gz_path = warc_gz_path
        self.local_test = local_test

        self.config = configparser.ConfigParser()
        self.config.read(cfg_path)

        if log_level:
            self.init_logging(log_level)
        else:
            self.init_logging(self.log_level)

    def get_s3_client(self):
        if not self.s3client:
            self.s3client = boto3.client('s3', use_ssl=False)
        return self.s3client

    def init_logging(self, level=None, session=None):
        if level:
            self.log_level = level
        else:
            level = self.log_level
        logging.basicConfig(level=level,
                            format=LOGGING_FORMAT)
        logging.getLogger(self.name).setLevel(level)
        if session:
            session.sparkContext.setLogLevel(level)

    # def get_logger(self, session=None):
    #     """Get logger from SparkSession or (if None) from logging module"""
    #     if not session:
    #         try:
    #             session = SparkSession.getActiveSession()
    #         except AttributeError:
    #             pass
    #     if session:
    #         return session._jvm.org.apache.log4j.LogManager \
    #                     .getLogger(self.name)
    #     return logging.getLogger(self.name)

    def process_warcs(self, _id, iterator):
        for path in iterator:
            stream = self.fetch_warc(path)
            if not stream:
                continue
            try:
                archive_iterator = ArchiveIterator(stream)
                for res in self.iterate_records(path, archive_iterator):
                    yield res
            except ArchiveLoadFailed as exception:
                print('Invalid WARC: {} - {}'.format(path, exception))
            finally:
                stream.close()

    def fetch_warc(self, path):
        warctemp = TemporaryFile(mode='w+b')
        stream = None
        # self.get_logger().info('Reading from S3 {}'.format(path))
        try:
            self.get_s3_client().download_fileobj(self.s3_bucket,
                                                  path, warctemp)
            warctemp.seek(0)
            stream = warctemp
        except botocore.client.ClientError as exception:
            print('Failed to download {}: {}'.format(path, exception))
            warctemp.close()
        return stream

    def iterate_records(self, _warc_path, archive_iterator):
        for record in archive_iterator:
            for res in self.process_record(record):
                yield res

    def process_record(record):
        """Process a single WARC record"""
        raise NotImplementedError('Processing record needs to be customized')

    def run_job(self, session):
        """Run the Spark Job"""
        raise NotImplementedError('Running the job needs to be customized')

    def run(self):

        session = SparkSession \
            .builder \
            .config("spark.jars.packages",
                    "com.johnsnowlabs.nlp:spark-nlp_2.11:1.8.2") \
            .config('spark.jars.packages',
                    'org.apache.hadoop:hadoop-aws:3.3.4') \
            .getOrCreate()

        session.sparkContext \
               ._jsc \
               .hadoopConfiguration() \
               .set("fs.s3a.access.key",
                    self.config['AWS']['AWS_ACCESS_KEY_ID'])
        session.sparkContext \
               ._jsc \
               .hadoopConfiguration() \
               .set("fs.s3a.secret.key",
                    self.config['AWS']['AWS_SECRET_ACCESS_KEY'])

        self.run_job(session)

        session.stop()


if __name__ == "__main__":
    pass
