# Project Goal

Create a data pipeline and data lake that will serve as the basis for
making queries about global (language-specific) textual news
content. In particular, the data resources are geared toward queries
tracking keyword emergence and life-cycle. 

For example, it should be easy to write a query to track the emergence
of the keywords "Climate Change" and discover when and where they
emerged; how their used developed in the context of the whole
text-news ecosystem; and what non-news domains were linked (via
literal hyperlink) to their use. 

I am particularly interested in using this dataset to see how
different news domains within similar ideological ecosystems get
"on-message" over time, and how that coherence changes around specific
events (i.e. elections).

# Data Model

![alt text](./Data_Model.png?raw=true)


# Technologies

The core technology used in this project is AWS EMR. The EMR cluster
is provisioned using Terraform and runs an ETL script written in
Python. 

The ETL script makes heavy use of the [cc-spark
library](https://github.com/commoncrawl/cc-pyspark), pyspark, and
[spark-nlp](https://nlp.johnsnowlabs.com/). 

The bootstrap and startup
scripts are written in bash, but are very basic. 

Lastly, data is read
from and written to AWS s3. 



