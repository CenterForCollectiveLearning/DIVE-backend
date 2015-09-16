import os
import sys
import logging
from logging import StreamHandler
from config import config

from app import app

def ensure_directories():
    if not os.path.isdir(app.config['UPLOAD_FOLDER']):
        app.info("Creating Upload directory")
        os.mkdir(app.config['UPLOAD_FOLDER'])

PORT = 8081
# http://stackoverflow.com/questions/11150343/slow-requests-on-local-flask-server
if __name__ == '__main__':
    ensure_directories()

    handler = StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    app.debug = True
    app.run(port=PORT)
