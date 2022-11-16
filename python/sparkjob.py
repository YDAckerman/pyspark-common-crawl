
from tempfile import TemporaryFile

from pyspark.sql import SparkSession

import boto3
import botocore

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

import configparser


class MySparkJob():

    name = 'SparkJob'
    s3client = None
    tests = []

    def __init__(self, output_path, s3_bucket='', warc_gz_path='',
                 cfg_path='', local_test=False):
        """
        - Instantiate MySparkJob object
        """
        self.s3_bucket = s3_bucket
        self.warc_gz_path = warc_gz_path
        self.output_path = output_path
        self.local_test = local_test

        self.config = configparser.ConfigParser()
        self.config.read(cfg_path)

    def get_s3_client(self):
        """
        - create/retrieve s3 client
        """
        if not self.s3client:
            self.s3client = boto3.client('s3', use_ssl=False)
        return self.s3client

    def process_warcs(self, _id, iterator):
        """
        - Loop through paths in partition (iterator)
        - Create stream from warc path (fetch_warc)
        - Create iterator of warc records from binary stream (ArchiveIterator)
        - Iterate through iterator of records
        """
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
        """
        - iterate through warc records in archive iterator
        - process each record
        """
        for record in archive_iterator:
            for res in self.process_record(record):
                yield res

    def process_record(record):
        """Process a single WARC record"""
        raise NotImplementedError('Processing record needs to be customized')

    def run_job(self, session):
        """Run the Spark Job"""
        raise NotImplementedError('Running the job needs to be customized')

    def run_tests(self, session):
        """
        Basic testing procedure. Just checks that data exists.
        """

        if self.tests == []:
            print('Nothing to test')
            pass

        test_pass = False
        for test in self.tests:
            test_count = session.read.parquet(self.output_path +
                                              test.table) \
                        .count()
            if test.operator == "=":
                test_pass = test_count == test.value
            elif test.operator == "<":
                test_pass = test_count < test.value
            elif test.operator == ">":
                test_pass = test_count > test.value

            if not test_pass:
                raise ValueError(f'Test of {test.table} failed')
            else:
                print(f'Test of {test.table} passed')

        pass

    def run(self):
        """
        - Start spark session
        - Configure aws access
        - run spark job
        - run data tests
        - close session
        """
        session = SparkSession \
            .builder \
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

        self.run_tests(session)

        session.stop()


if __name__ == "__main__":
    pass
