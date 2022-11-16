import argparse
from newsjob import NewsJob
from graphjob import GraphJob


def parse_arguments():
    # get cfg path from args or load default
    parser = argparse.ArgumentParser()
    parser.add_argument("--cfg_path", help="Path to your aws config file.")
    args = parser.parse_args()
    return args


def main():

    cc_bucket = 'commoncrawl'
    news_paths = 'crawl-data/CC-NEWS/*/*/warc.paths.gz'
    output_path = 's3://commoncrawl-news-tables/'
    args = parse_arguments()
    cfg_path = 'aws.cfg' if not args.cfg_path else args.cfg_path

    # news_job = NewsJob(output_path=output_path, s3_bucket=cc_bucket,
    #                    warc_gz_paths=news_paths, cfg_path=cfg_path,
    #                    local_test=True)
    # news_job.run()

    graph_job = GraphJob(output_path=output_path,
                         cfg_path=cfg_path)
    graph_job.run()

if __name__ == "__main__":
    main()
