import re

from tabletest import TableTest
from sparkjob import MySparkJob

from urllib.parse import urlparse
from bs4 import BeautifulSoup

# may need to (pip uninstall lxml; pip install lxml)
from htmldate import find_date

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, explode, arrays_zip, to_date, \
    dayofmonth, year, month, udf

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline

# import pdb


class NewsJob(MySparkJob):

    name = "NewsJob"

    output_schema = StructType([
        StructField("domain", StringType(), True),
        StructField("warc_date", StringType(), True),
        StructField("publish_date", StringType(), True),
        StructField("article_text", StringType(), True)
    ])

    tests = [TableTest('sites_table/', ">", 0),
             TableTest('dates_table/', ">", 0),
             TableTest('keywords_table/', ">", 0)]

    def process_record(self, record):
        """
        - extract the publish date
        - extract the article text
        - extract the warc read date
        - extract the site domain
        """
        if record.rec_type != 'response':
            return

        html = record.content_stream().read()
        # get publish date
        pub_date = find_date(html)
        # get article text
        soup = BeautifulSoup(html, features='html.parser')
        for script in soup(["script", "style"]):
            script.extract()
        text = soup.get_text()
        # get warc date
        warc_date = record.rec_headers['WARC-Date']
        # get site domain
        dom = urlparse(record.rec_headers['WARC-Target-URI']).netloc

        yield dom, warc_date, pub_date, text

    def language_detect_pipeline(self):
        """
        Copied nearly verbatim from:
        https://nlp.johnsnowlabs.com/docs/en/annotators#languagedetectordl

        Creates pipeline to perform language detection
        """
        documentAssembler = DocumentAssembler() \
            .setInputCol("article_text") \
            .setOutputCol("document")

        languageDetector = LanguageDetectorDL.pretrained() \
                                             .setInputCols("document") \
                                             .setOutputCol("language")

        pipeline = Pipeline() \
            .setStages([
                documentAssembler,
                languageDetector
            ])

        return pipeline

    def keyword_extract_pipeline(self):
        """
        Copied nearly verbatim from:
        https://nlp.johnsnowlabs.com/docs/en/annotators#yakekeywordextraction

        Creates pipeline to do keyword extraction
        """
        documentAssembler = DocumentAssembler() \
            .setInputCol("article_text") \
            .setOutputCol("document")

        sentenceDetector = SentenceDetector() \
            .setInputCols(["document"]) \
            .setOutputCol("sentence")

        token = Tokenizer() \
            .setInputCols(["sentence"]) \
            .setOutputCol("token") \
            .setContextChars(["(", "]", "?", "!", ".", ","])

        keywords = YakeKeywordExtraction() \
            .setInputCols(["token"]) \
            .setOutputCol("keywords") \
            .setThreshold(0.6) \
            .setMinNGrams(2) \
            .setNKeywords(5)

        pipeline = Pipeline().setStages([
            documentAssembler,
            sentenceDetector,
            token,
            keywords
        ])

        return pipeline

    def run_job(self, session):
        """
        - Get warc paths from s3
        - Partition and process warc paths
        - Transform results
        - Get all metadata and filter down to news domains
        - Save to s3
        """

        # get all warc paths
        input_data = session \
            .sparkContext \
            .textFile(f's3://{self.s3_bucket}/{self.warc_gz_path}')

        # when running locally, operate on subset of paths
        if self.local_test:
            input_data = session.sparkContext.parallelize(input_data.take(1))

        # process the warcs
        news_rdd = input_data.mapPartitionsWithIndex(self.process_warcs)

        # create a new data frame from the results
        news_df = session.createDataFrame(news_rdd,
                                          schema=self.output_schema)

        # perform language detection
        news_df = self.language_detect_pipeline().fit(news_df) \
                                                 .transform(news_df)

        @udf(returnType=StringType())
        def reverse_domain(s):
            """
            - removes ^www
            - reverses what's left around '.'
            """
            s = '.'.join(re.sub('^www.', '', s).split(".")[-1::-1])
            return s

        # create sites table and write to s3
        sites = news_df \
            .selectExpr('domain', 'language.result as languages') \
            .withColumn('reverse_domain', reverse_domain('domain')) \
            .distinct()
        sites.write \
             .mode('overwrite').parquet(self.output_path + 'sites_table/')

        # create dates table and write to s3
        dates = news_df.select(col('publish_date'),
                               to_date(col('publish_date'), "yyyy-MM-dd")
                               .alias('date')) \
                       .withColumn('day', dayofmonth('date')) \
                       .withColumn('year', year('date')) \
                       .withColumn('month', month('date')) \
                       .distinct()
        dates.write \
             .partitionBy('year', 'month') \
             .mode('overwrite') \
             .parquet(self.output_path + 'dates_table/')

        # perform keyword extraction, create keywords table, write to s3
        keywords = self.keyword_extract_pipeline() \
                       .fit(news_df) \
                       .transform(news_df) \
                       .selectExpr('domain',
                                   'publish_date',
                                   'warc_date',
                                   'explode(arrays_zip(keywords.result, '
                                   'keywords.metadata)) as resultTuples') \
                       .selectExpr('domain',
                                   'publish_date',
                                   'warc_date',
                                   "resultTuples['0'] as keyword",
                                   "resultTuples['1'].score as score")
        keywords.write \
                .partitionBy('domain') \
                .mode('overwrite') \
                .parquet(self.output_path + 'keywords_table/')

        pass


if __name__ == "__main__":
    pass
