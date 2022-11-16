

from tabletest import TableTest
from sparkjob import MySparkJob

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import array_contains


class GraphJob(MySparkJob):

    name = "GraphJob"

    hyperlink_path = 's3://commoncrawl/projects/' \
        + 'hyperlinkgraph/cc-main-2022-may-jun-aug/' \
        + 'domain/'

    node_path = 'cc-main-2022-may-jun-aug-domain-vertices.txt.gz'
    edge_path = 'cc-main-2022-may-jun-aug-domain-edges.txt.gz'

    node_schema = StructType([
        StructField("id", StringType(), True),
        StructField("reverse_domain", StringType(), True),
        StructField("num_hosts", StringType(), True)
    ])

    edge_schema = StructType([
        StructField("from_id", StringType(), True),
        StructField("to_id", StringType(), True)
    ])

    tests = [TableTest('graph_nodes/', ">", 0),
             TableTest('news_graph_edges/', ">", 0)]

    def map_line(self, line):
        """
        This function is verbatim from the cc-spark examples
        - break line on tab character
        """
        return line.split('\t')

    def run_job(self, session):
        """
        - get all news domains
        - get all nodes (domains)
        - filter nodes down to english nodes
        - get all ids of english nodes
        - get all edges (from, to)
        - filter down edges to those containing an english news node id
        - write nodes and english-news node-edges to s3
        """

        sites = session.read.parquet(self.output_path + 'sites_table/')
        english_nodes = sites \
            .filter(array_contains(sites.languages, "en")) \
            .select('reverse_domain') \
            .distinct()

        node_data = session \
            .sparkContext \
            .textFile(self.hyperlink_path + self.node_path,
                      minPartitions=400) \
            .map(self.map_line)
        node_df = session.createDataFrame(node_data,
                                          schema=self.node_schema)
        node_df \
            .write \
            .mode('overwrite') \
            .parquet(self.output_path + 'graph_nodes/')

        english_node_ids = node_df.join(english_nodes,
                                        'reverse_domain',
                                        'leftsemi') \
                                  .select('id') \
                                  .collect()
        eng_nodes = []
        for row in english_node_ids:
            eng_nodes.append(row['id'])

        edge_data = session \
            .sparkContext \
            .textFile(self.hyperlink_path + self.edge_path,
                      minPartitions=400) \
            .map(self.map_line)
        edge_df = session.createDataFrame(edge_data,
                                          schema=self.edge_schema)
        news_edges = edge_df.filter(edge_df.from_id.isin(eng_nodes) |
                                    edge_df.to_id.isin(eng_nodes))
        news_edges \
            .write \
            .mode('overwrite') \
            .parquet(self.output_path + 'news_graph_edges/')

        pass


if __name__ == "__main__":
    pass
