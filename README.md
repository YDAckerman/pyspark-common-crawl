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

Note: the nodes in the Edges portion of the dataset are drawn only
from news domains that use English and domains linking directly to or from
them. 

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

# Setup

Code and commented instructions for setup are provided in 
aws-emr/bash/startup.sh. 

# Process

First I engaged in an initial planning phase. I determined what data I
wanted to draw from and what questions I would want to ask. 

I knew I would be using spark for this project, so I then went about
learning how to provision an EMR cluster with Terraform. I had done a
bare-bones version before, but I needed to figure out how to import
additional python libraries (warcio) and third-party spark packages
(spark-nlp). Thus the next steps were creating an s3 bucket to hold
bootstrap.sh and getting the correct configuration json in main.tf
(there is a good example on the spark-nlp website). 

Understanding the cc-spark module and pulling out only the things I
specifically wanted came next. I don't think this was strictly
necessary. But as I both wanted to better understand the module's
structure and get more experience writing/testing code myself,
it's what I ended up doing. I would not have been able to approach
this problem without heavily drawing on cc-spark code. 

Once I could comfortably access/process the .warc files on EMR, I went
about getting spark-nlp algorithms for language detection and keyword
extraction working. This ultimately involved setting up the correct
version of EMR and including the correct jars package path in the
main.tf configuration json. 

The final steps were to write the newsjob and graphjob classes, test
them, and then organize the whole ETL process into etl.py

Sidenote: the actual process was not quite this linear. I tried
designing the project a-priori as best as I could, but I ran into
quite a few unknown-unknowns and thus necessary re-designs. 





