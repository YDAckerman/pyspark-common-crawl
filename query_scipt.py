import configparser

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import array_contains, size, col, desc

config = configparser.ConfigParser()
config.read('./aws.cfg')

session = SparkSession.builder \
    .getOrCreate()

session.sparkContext \
       ._jsc \
       .hadoopConfiguration() \
       .set("fs.s3a.access.key",
            config['AWS']['AWS_ACCESS_KEY_ID'])
session.sparkContext \
       ._jsc \
       .hadoopConfiguration() \
       .set("fs.s3a.secret.key",
            config['AWS']['AWS_SECRET_ACCESS_KEY'])

s3_path = 's3://commoncrawl-news-tables/'

# connect to the tables

dates_df = session \
    .read \
    .parquet(s3_path + 'dates_table/')

keywords_df = session \
    .read \
    .parquet(s3_path + 'keywords_table')

sites_df = session \
    .read \
    .parquet(s3_path + 'sites_table/')

nodes_df = session \
    .read \
    .parquet(s3_path + 'graph_nodes/')

edge_df = session \
    .read \
    .parquet(s3_path + 'news_graph_edges/')


# We pulled a random section of the news crawl data
# so let's see what we got time-wise

keywords_df.join(dates_df, 'publish_date', 'left') \
    .select('domain', 'year') \
    .distinct() \
    .groupBy('year') \
    .count() \
    .orderBy(desc('count')) \
    .show()

# +----+-----+
# |year|count|
# +----+-----+
# |2016| 1435|
# |2015|  237|
# |2014|  107|
# |2013|   70|
# |null|   60|
# |2012|   43|
# |2011|   34|
# |2010|   17|
# |2009|   15|
# |2008|    6|
# |2017|    5|
# |2004|    5|
# |2006|    5|
# |2020|    5|
# |2007|    4|
# |1997|    2|
# |2005|    2|
# |1999|    2|
# |2021|    2|
# |2019|    2|
# +----+-----+
# only showing top 20 rows

# Let's focus on 2016, as that is when most of our data
# is from. I will also want to go back and have a look
# at where the null values are coming from.

dates_subset = dates_df.filter(dates_df.year == 2016)

# filter down english

sites_subset = sites_df \
    .filter(array_contains(col('languages'), "en")) \
    .filter(size(col('languages')) == 1)

# semi join against keywords table

keywords_subset = keywords_df \
    .join(dates_subset, 'publish_date', 'leftsemi') \
    .join(sites_subset, 'domain', 'leftsemi')

# look at top keywords

top_keywords_subset = keywords_subset \
    .groupBy('keyword') \
    .count() \
    .orderBy(desc('count')) \
    .show()

# +------------------+-----+
# |           keyword|count|
# +------------------+-----+
# |     email address|29239|
# |         temp high|24835|
# |        local news|17797|
# |             de la|15921|
# |        vezi toate|10350|
# |     toate stirile|10230|
# |vezi toate stirile|10200|
# |          le point| 8735|
# |         sports bc| 8611|
# |         aug sunny| 8276|
# |   newsletter sign| 7980|
# |        west speed| 6762|
# |         hours ago| 4718|
# |      bc sports bc| 4521|
# |   navigation jump| 4310|
# |      news bc news| 4273|
# |    bc news sports| 4227|
# |    news sports bc| 4181|
# |  sports bc sports| 3805|
# |    edition switch| 3355|
# +------------------+-----+
# only showing top 20 rows

# well these are weird! Maybe we need to
# filter to top keyword within an article
# before doing this...

# we covered window functions in the course
# but I used this stackoverflow example as a
# reminder: https://stackoverflow.com/questions/48829993

w = Window.partitionBy('domain')
keywords_subset \
    .withColumn('top_score', F.max('score').over(w)) \
    .filter(col('score') == col('top_score')) \
    .groupBy('keyword') \
    .count() \
    .orderBy(desc('count')) \
    .show()

# +--------------------+-----+
# |             keyword|count|
# +--------------------+-----+
# |    fabian radulescu|  182|
# |      emanuel county|  133|
# |      millionen euro|  132|
# |forgotten your pa...|  116|
# |     street junction|   79|
# |       putnam county|   71|
# |        sierra leone|   70|
# |         scale forms|   58|
# | reset your password|   55|
# |          angel fire|   52|
# |       isthmus picks|   44|
# |       coventry city|   43|
# |    facebook twitter|   41|
# |   huddersfield town|   38|
# |        rbc heritage|   37|
# |   manchester united|   37|
# |      sidi bel abbès|   32|
# |       joshua bessex|   29|
# |      inmates booked|   27|
# |         use cookies|   24|
# +--------------------+-----+
# only showing top 20 rows

# These make more sense, but I still think we must
# have pulled an odd bunch of records! Or we've got
# some algorithmic artifacts...

# how about looking at all sites that
# link to news domains that mention 'climate change'

keywords_by_keyword = keywords_df \
    .filter(col('keyword').contains('climate')) \
    .groupBy('keyword') \
    .count() \
    .orderBy(desc('count'))

# +--------------------+-----+
# |             keyword|count|
# +--------------------+-----+
# |      climate change|  224|
# |  design for climate|    9|
# |     climate concept|    9|
# |  investment climate|    8|
# |  climate scientists|    6|
# |better investment...|    6|
# |west antarctic cl...|    6|
# |  australian climate|    5|
# |       paris climate|    5|
# |   political climate|    5|
# |   climate agreement|    5|
# |paris climate agr...|    5|
# |new political cli...|    5|
# |    climate skeptics|    3|
# |climate change began|    2|
# |        cold climate|    2|
# |    us agree climate|    2|
# |ratify paris climate|    2|
# |    climate approach|    2|
# |climate concept town|    1|
# +--------------------+-----+
# only showing top 20 rows

# Nice to note that the keyword extraction doesn't
# always put 'paris climate' together with 'aggreement'
# or 'climate agreement'. For now let's just proceed
# with 'climate change' as our keyword. (we could also
# ask what the relative rank of the keyword 'climate change'
# is whenever it appears, but I'm not sure how much information
# that will give us. Anways, onward...)

domains_climate = keywords_df \
    .filter(col('keyword') == 'climate change')

sites_climate = sites_df \
    .join(domains_climate, 'domain', 'leftsemi')

nodes_climate = nodes_df \
    .join(sites_climate, 'reverse_domain', 'leftsemi')

edges_climate = edge_df \
    .join(nodes_climate, edge_df.to_id == nodes_climate.id,
          'leftsemi')

nodes_climate_context = nodes_df \
    .join(edges_climate, edges_climate.from_id == nodes_df.id,
          'inner')

nodes_climate_context.show()

# +--------+--------------------+---------+--------+--------+
# |      id|      reverse_domain|num_hosts| from_id|   to_id|
# +--------+--------------------+---------+--------+--------+
# |10000781|       com.agemobile|        3|10000781|46843547|
# |10011851|    com.agencyheight|        3|10011851|46843547|
# |10012644|       com.agencyuae|        1|10012644|46843547|
# |10021717| com.agenziaradicale|        1|10021717|46843547|
# |10024689|     com.agftutoring|        1|10024689|46843547|
# |10029792|      com.agileandco|        2|10029792|46843547|
# |10034327|          com.aging2|        2|10034327|46843547|
# |10038627|com.aglobalnomads...|        1|10038627|46843547|
# |10040466|   com.agnafrica2030|        1|10040466|46843547|
# |10051534|com.agreementexpress|        6|10051534|46843547|
# |10054975|com.agriculturalw...|        1|10054975|46843547|
# |10065282|       com.agropolit|        1|10065282|46843547|
# |10075561|      com.agutsygirl|        1|10075561|46843547|
# |10085130|         com.aheadoh|        1|10085130|46843547|
# |10085521|com.ahealthybluep...|        1|10085521|46843547|
# |10097446|       com.ahmediatv|        1|10097446|46843547|
# |10099060|          com.ahmnet|        1|10099060|46843547|
# |10101668|       com.ahoraleon|        1|10101668|46843547|
# |10102127|   com.ahorrocapital|        3|10102127|46843547|
# |10104131|      com.ahr-global|        5|10104131|46843547|
# +--------+--------------------+---------+--------+--------+
# only showing top 20 rows

# So this is actually totally meaningless. I draw the graph data
# from the 2022 dataset, so these nodes don't necessarily link
# to the news nodes in the context of climate. As a proof of
# concept it is fine, but if I were to actually make something
# from this I would need to make sure the .warc and graph data I'm
# pulling are aligned temporally. Furthermore, it would be most
# interesting to see which non-news pages are linked to news pages
# that contain the keyword of interest. So we'd really need to
# go through all the hyper links (in the from_id node) and see
# if any of them specifically hit a page in the to_id node that
# contains 'climate change'. A project for another day!
