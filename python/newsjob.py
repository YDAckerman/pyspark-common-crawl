from urllib.parse import urlparse
from bs4 import BeautifulSoup

# may need to (pip uninstall lxml; pip install lxml)
from htmldate import find_date

from sparkjob import MySparkJob

from pyspark.sql.types import StructType, StructField, StringType


class NewsJob(MySparkJob):

    output_schema = StructType([
        StructField("domain", StringType(), True),
        StructField("publish_date", StringType(), True),
        StructField("article_text", StringType(), True)
    ])

    def process_record(record):
        if record.rec_type != 'response':
            return
        html = record.content_stream().read()
        pub_date = find_date(html)
        soup = BeautifulSoup(html, features='html.parser')
        for script in soup(["script", "style"]):
            script.extract()
        text = soup.get_text()
        dom = urlparse(record.rec_headers['WARC-Target-URI']).netloc
        yield dom, pub_date, text

    def run_job(self, session):

        input_data = session \
            .sparkContext \
            .textFile(f's3a://{self.s3_bucket}/{self.warc_gz_path}')

        # when running locally, keep the data size managable.
        if self.local_test:
            input_data = input_data.sample(False, 1/input_data.count())

        news_rdd = input_data.mapPartitionsWithIndex(self.process_warcs)

        news_df = session.createDataFrame(news_rdd, schema=self.output_schema)

        
            
