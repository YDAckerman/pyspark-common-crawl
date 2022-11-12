import argparse
from newsjob import NewsJob


if __name__ == "__main__":

    cc_bucket = 'commoncrawl'
    news_paths = 'crawl-data/CC-NEWS/*/*/warc.paths.gz'
    output_path = 's3a://commoncrawl-news-tables/'

    # get cfg path from args or load default
    parser = argparse.ArgumentParser()
    parser.add_argument("--cfg_path", help="Path to your aws config file.")
    args = parser.parse_args()

    cfg_path = 'aws.cfg' if not args.cfg_path else args.cfg_path

    NewsJob(cc_bucket, news_paths, cfg_path, output_path,
            local_test=True).run()
