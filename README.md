# Project Goal

The project goal was to create a data pipeline and data lake to serve as the basis for
making queries about global (language-specific) textual news
content via keyword searching. In particular, the data resources are
geared toward queries tracking keyword emergence and life-cycle. 

For example, it should be easy to write a query to track the emergence
of the keywords "Climate Change" and discover when and where they
emerged; how their use developed in the context of the whole
text-news ecosystem over time; and what non-news domains were linked (via
literal hyperlink) to their use. 

I am particularly interested in using this dataset to see how
different news domains within similar ideological ecosystems get
"on-message" over time, and how that coherence changes around specific
events (i.e. elections).

Note: the nodes in the Edges portion of the dataset are drawn only
from news domains that use English and domains linking directly to or from
them. 

Finally: this is a capstone project for the Udacity Data Engineering
Nanodegree. Unlike the other course projects, this one is entirely of
my own design. Thank you to Udacity for an excellent learning
experience and the mentor feedback along the way. 

# Original Data Source

The data is drawn from the [commoncrawl](https://commoncrawl.org/)
dataset. What they have made publicly available is absolutely
amazing. Their documentation is great and there is a very nice
ecosystem of tools (cc-spark, in particular) that make using their
data fairly straight forward. 

# Data Model

![alt text](./Data_Model.png?raw=true)

# Technologies

The core technology used in this project is AWS EMR. The EMR cluster
is provisioned using Terraform and runs an ETL script written in
Python. 

The ETL script makes heavy use of the [cc-spark
module](https://github.com/commoncrawl/cc-pyspark), pyspark, and
[spark-nlp](https://nlp.johnsnowlabs.com/). 

The bootstrap and startup
scripts are written in bash, but are very basic. They do require that
your system as the aws cli installed. 

Lastly, data is read
from and written to AWS s3. 

# Setup

Code and commented instructions for setup are provided in 
aws-emr/bash/startup.sh. 

The NewsJob instance in etl.py is currently set to
localTest=True. If you set it to False or remove it, you will need to
scale up the number of worker nodes and consider changing the number
of data partitions within the NewsJob run_job() function. Even with
the test settings, my run of the script took 8+ hours to complete and
cost me approximately $10 in aws fees. 

The output_path in etl.py is currently set to one of my s3 buckets. You
will need to create your own bucket and change this variable before
running this yourself. 

# Process

First I engaged in an initial planning phase. I determined what data I
wanted to draw from (commoncrawl) and what questions I would want to ask. 

I knew I would be using spark for this project, so I then went about
learning how to provision an EMR cluster with Terraform. I had done a
bare-bones version before, but I needed to figure out how to import
additional python libraries (warcio) and third-party spark packages
(spark-nlp). Thus the next steps were creating an s3 bucket to hold
bootstrap.sh and getting the correct configuration json in main.tf
(there is a good example on the spark-nlp website). 

Understanding the cc-spark module and pulling out only the things I
specifically wanted came next. This was not at all necessary. cc-spark
can pretty much be used off the shelf, as long as you've got your
cluster provisioned properly. But as I both wanted to better understand the module's
structure and get more experience writing/testing code myself,
it's what I ended up doing. I would not have been able to approach
this problem without heavily drawing on cc-spark code. In some places
I use their code pretty much verbatim.

Once I could comfortably access/process the .warc files on EMR, I went
about getting spark-nlp algorithms for language detection and keyword
extraction working. This ultimately involved setting up the correct
version of EMR and including the correct .jars paths in the
main.tf configuration json. I only did off-the-shelf things with
spark-nlp. I used the language detection and yakekeywordextraction
example pipelines verbatim. 

The final steps were to write the newsjob and graphjob classes, test
them, and then organize the whole ETL process into etl.py

Sidenote: the actual process was not this linear. I tried
designing the project a-priori as best as I could, but I ran into
quite a few unknown-unknowns and thus necessary re-designs. 

# Follow Up

1. If data were scaled up 100x: 
    - If we wanted to access 100x more .warc files or the graph data
      increased 100x, we would want to increase the number of data
      partitions (at session.sparkContext.textFile) and
      add more workers to the cluster. If the individual .warc files
      increased 100x in size, we would need to increase the RAM on the
      EMR nodes. If the individuals .warc files got too big for our
      budget or (something like that), we could use the cc-index
      parquet tables to figure out which .warc files contain the data
      we want, then use the data segments .warc files (limited to
      10000 records a piece) to access the records we want. This,
      however, would require filtering out non-news-domain records.
    - As of now, there are not that many news sites in the commoncrawl
      dataset. Because of this, I made a .collect() call in the
      graphjob and used the results to filter the graph nodes. If this
      number increased 100x, the code would need to change. We would
      need to create a dataframe of news\_ids, join them into the edge
      dataframe twice, once on to\_id and once on from\_id,
      then filter out rows that have nulls in both of the
      newly-joined columns.
2. If ETL needs to be done on a daily basis, I would need to provision
   some form of task orchestrator, airflow or prefect etc. The code
   would then need to be adjusted (in the case of airflow) to
   only pull that day's news crawl using context variables. I have not
   set up an airflow EMR operator, so I'd need to figure that out and
   how it interacts with terraform to create and destroy the
   cluster. 
3. If the database needed to be accessed by 100 people, I think the
   main issue would be controlling permissions via IAM roles. Given
   that the data is on s3, I don't think there would be an issue. Load
   balancing in that case is handled by Amazon. 
      




