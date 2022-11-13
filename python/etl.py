import argparse
from newsjob import NewsJob


def parse_arguments():
    # get cfg path from args or load default
    parser = argparse.ArgumentParser()
    parser.add_argument("--cfg_path", help="Path to your aws config file.")
    args = parser.parse_args()
    return args


def main():
    cc_bucket = 'commoncrawl'
    news_paths = 'crawl-data/CC-NEWS/*/*/warc.paths.gz'
    output_path = 's3a://commoncrawl-news-tables/'
    args = parse_arguments()

    cfg_path = 'aws.cfg' if not args.cfg_path else args.cfg_path

    news_job = NewsJob(cc_bucket, news_paths, cfg_path, output_path,
                       local_test=True)
    news_job.run()


if __name__ == "__main__":
    main()
