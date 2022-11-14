import configparser
import os

from tempfile import TemporaryFile

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import split, col, explode, desc
from pyspark.sql.types import StructType, StructField, StringType

config = configparser.ConfigParser()
config.read('./sandbox.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

spark = SparkSession.builder \
    .config("spark.jars.packages",
            "com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.3") \
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

def fetch_warc(path):
    # path = 'crawl-data/CC-NEWS/2022/03/CC-NEWS-20220325175840-00065.warc.gz'
    warctemp = TemporaryFile(mode='w+b')
    stream = None
    try:
        boto3.client('s3').download_fileobj(cc_bucket,
                                            path,
                                            warctemp)
        warctemp.seek(0)
        stream = warctemp
    except botocore.client.ClientError as exception:
        print('Failed to download {}: {}'.format(path, exception))
        warctemp.close()
    return stream


def iterate_records(_warc_uri, archive_iterator):
    for record in archive_iterator:
        for res in process_record(record):
            yield res

            
def process_record(record):
    if record.rec_type != 'response':
        # skip over WARC request or metadata records
        return
    # page = record.content_stream.read()
    html = record.content_stream().read()
    pub_date = find_date(html)
    soup = BeautifulSoup(html, features='html.parser')
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    dom = urlparse(record.rec_headers['WARC-Target-URI']).netloc
    yield dom, pub_date, text

# def process_record(record):
#     # record = next(archive_iterator)
#     if record.rec_type != 'response':
#         # skip over WARC request or metadata records
#         return
#     # page = record.content_stream.read()
#     dom = urlparse(record.rec_headers['WARC-Target-URI']).netloc
#     yield dom, 1

# NOTE s3a -> s3 on EMR
cc_segment_paths = spark \
    .sparkContext \
    .textFile(f's3a://{cc_bucket}/{cc_segment_paths}')

news_input = spark \
    .sparkContext \
    .textFile(f's3a://{cc_bucket}/{news_paths}')
news_input = news_input.sample(False, 1/news_input.count())

output_schema = StructType([
        StructField("domain", StringType(), True),
        StructField("publish_date", StringType(), True),
        StructField("article_text", StringType(), True)
    ])

news = news_input.mapPartitionsWithIndex(process_warcs)
news_df = spark.createDataFrame(news, schema=output_schema)




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
    .setThreshold(0.6) \
    .setMinNGrams(2) \
    .setNKeywords(10)

pipeline = Pipeline().setStages([
    documentAssembler,
    sentenceDetector,
    token,
    keywords
])


news_df.take(5)
