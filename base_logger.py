import logging

from argparse import Namespace
import yaml
from pathlib import Path
from re import sub


"""
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--vectorize", action='store_true')
    parser.add_argument("-c", "--cooccurrence", action='store_true')
    parser.add_argument("-t", "--train-model", action='store_true')
    return parser.parse_args()
"""

def load_config():
    try:
        config_filepath = Path(__file__).absolute().resolve().parent / "config.yaml"
        with config_filepath.open() as f:
            config_dict = yaml.load(f, Loader=yaml.FullLoader)
        config = Namespace()
        for key, value in config_dict.items():
            setattr(config, key, value)
        return config
    except Exception as ex:
        return None

config=load_config()

def get_level(x):
    return {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING ,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }.get(x, logging.INFO)

logging_level=logging.INFO
if config is not None and hasattr(config, 'logging_level'):
    logging_level=get_level(config.logging_level)
file_log='newLog'
if config is not None and hasattr(config, 'log_file') and sub(r'[^a-zA-Z0-9]', '', config.log_file):
	file_log=sub(r'[^a-zA-Z0-9]', '', config.log_file)
file_log+='.log'

logger = logging
logger.basicConfig(format='[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s', level=logging_level, filename=file_log, encoding='utf-8')