from urllib.parse import urlparse
from bs4 import BeautifulSoup

# may need to (pip uninstall lxml; pip install lxml)
from htmldate import find_date

from sparkjob import MySparkJob

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import split, col, explode, desc

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline

import pdb

class NewsJob(MySparkJob):

    name = "NewsJob"

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

    def language_detect_pipeline():
        """
        code citation here
        """
        documentAssembler = DocumentAssembler() \
            .setInputCol("text") \
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

    def keyword_extract_pipeline():
        """
        code citation here
        """
        documentAssembler = DocumentAssembler() \
            .setInputCol("text") \
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
            .setThreshold(0.6) \ # from example documentation
            .setMinNGrams(2) \ # default
            .setNKeywords(10) # might want to extract more

        pipeline = Pipeline().setStages([
            documentAssembler,
            sentenceDetector,
            token,
            keywords
        ])

        return pipeline

    def run_job(self, session):

        # pdb.set_trace()

        input_data = session \
            .sparkContext \
            .textFile(f's3a://{self.s3_bucket}/{self.warc_gz_path}')

        # when running locally, keep the data size managable.
        if self.local_test:
            input_data = input_data.sample(False, 1/input_data.count())

        news_rdd = input_data.mapPartitionsWithIndex(self.process_warcs)
        news_df = session.createDataFrame(news_rdd,
                                          schema=self.output_schema)

        news_df = news_df.withColumn('publish_date',
                                     to_date(col('publish_date',"yyyy-MM-dd"))
        news_df = language_detect_pipeline().fit(news_df).transform(data)

        sites = news_df.select('domain', 'language.result').distinct()
                                     
        dates = news_df.select('publish_date',
                               to_date(col('publish_date',"yyyy-MM-dd")) \
                               .withColumn('day', F.dayofmonth('publish_date')) \
                               .withColumn('year', F.year('publish_date')) \
                               .withColumn('month', F.month('publish_date')) \
                               .distinct()

        news_df = keyword_extract_pipeline().fit(news_df).transform(data)

