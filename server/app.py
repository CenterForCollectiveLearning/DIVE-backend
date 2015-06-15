import os
from flask import Flask
from config import config

app = Flask(__name__, static_url_path='/static')
app.config['SERVER_NAME'] = "localhost:8888"

TEST_DATA_FOLDER = os.path.join(os.curdir, config['TEST_DATA_FOLDER'])
app.config['TEST_DATA_FOLDER'] = TEST_DATA_FOLDER

PUBLIC_DATA_FOLDER = os.path.join(os.curdir, config['PUBLIC_DATA_FOLDER'])
app.config['PUBLIC_DATA_FOLDER'] = PUBLIC_DATA_FOLDER

UPLOAD_FOLDER = os.path.join(os.curdir, config['UPLOAD_FOLDER'])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

STATIC_FOLDER = os.path.join(os.curdir, config['STATIC_FOLDER'])
app.static_folder = STATIC_FOLDER
