import argparse
import configparser
import os


# get cfg path from args or load default
parser = argparse.ArgumentParser()
parser.add_argument("--cfg_path", help="Path to your aws config file.")
args = parser.parse_args()
config = configparser.ConfigParser()
if args.cfg_path:
    config.read(args.cfg_path)
else:
    config.read("aws.cfg")

# setup aws configuration
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
