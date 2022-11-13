from urllib.parse import urlparse
from bs4 import BeautifulSoup

# may need to (pip uninstall lxml; pip install lxml)
from htmldate import find_date

from sparkjob import MySparkJob

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, explode, arrays_zip

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline

# import pdb


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

        keywords = YakeKeywordExtraction(threshold=.6,
                                         minNGrams=2,
                                         nKeywords=10) \
            .setInputCols(["token"]) \
            .setOutputCol("keywords")

        pipeline = Pipeline().setStages([
            documentAssembler,
            sentenceDetector,
            token,
            keywords
        ])

        return pipeline

    def run_job(self, session):

        # pdb.set_trace()

        # get all warc paths
        input_data = session \
            .sparkContext \
            .textFile(f's3a://{self.s3_bucket}/{self.warc_gz_path}')

        # when running locally, operate on subset of paths
        if self.local_test:
            input_data = session.sparkContext.parallelize(input_data.take(1))

        # process the warcs
        news_rdd = input_data.mapPartitionsWithIndex(self.process_warcs)

        # create a new data frame from the results
        news_df = session.createDataFrame(news_rdd,
                                          schema=self.output_schema)

        # perform language detection
        news_df = self.language_detect_pipeline().fit(news_df).transform(news_df)

        # create sites table and write to s3
        sites = news_df.select('domain', 'language.result').distinct()
        sites.write.mode('overwrite').parquet(self.output_path + 'sites_table/')

        # create dates table and write to s3
        dates = news_df.select('publish_date',
                               F.to_date(col('publish_date'), "yyyy-MM-dd")) \
                               .withColumn('day', F.dayofmonth('publish_date')) \
                               .withColumn('year', F.year('publish_date')) \
                               .withColumn('month', F.month('publish_date')) \
                               .distinct()
        dates.write.mode('overwrite').parquet(self.output_path + 'dates_table/')

        # perform keyword extraction, create keywords table, write to s3
        keywords = self.keyword_extract_pipeline() \
                               .fit(news_df) \
                               .transform(news_df) \
                               .selectExpr('domain',
                                           'publish_date',
                                           'explode(arrays_zip(keywords.result, '
                                           'keywords.metadata)) as resultTuples') \
                               .selectExpr('domain',
                                           'publish_date',
                                           "resultTuples['0'] as keyword",
                                           "resultTuples['1'].score as score")
        keywords.write \
                .partitionBy('domain') \
                .mode('overwrite') \
                .parquet(self.output_path + 'keywords_table/')

        pass


if __name__ == "__main__":
    pass
