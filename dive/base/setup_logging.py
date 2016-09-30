'''
Set-up logging entry point given config file
'''
import os
import yaml
import logging
import logging.config

def setup_logging(default_path='logging.yaml', default_level=logging.DEBUG):
    path = default_path
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
