import os
import sys
import logging
from celery import Celery
from flask import Flask, request
from flask.ext.sqlalchemy import SQLAlchemy
from werkzeug.local import LocalProxy


def ensure_directories(app):
    if not os.path.isdir(app.config['UPLOAD_FOLDER']):
        app.logger.info("Creating Upload directory")
        os.mkdir(app.config['UPLOAD_FOLDER'])

logger = None
config = None
db = SQLAlchemy()

# See https://github.com/spendb/spendb/blob/da042b19884e515eb15e3d56fda01b7b94620983/spendb/core.py
def create_app(**config):
    app = Flask(__name__)
    app.config.from_object('config.DevelopmentConfig')
    config = app.config

    from api import api
    api.init_app(app)

    db.init_app(app)

    # db.reflect()
    # db.drop_all()
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    logger = app.logger

    ensure_directories(app)
    return app


def create_celery(app):
    celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    return celery


# @app.before_request
# def option_autoreply():
#     """ Always reply 200 on OPTIONS request """
#     if request.method == 'OPTIONS':
#         resp = app.make_default_options_response()
#
#         print "Here comes an OPTIONS request"
#
#         headers = None
#         if 'ACCESS_CONTROL_REQUEST_HEADERS' in request.headers:
#             headers = request.headers['ACCESS_CONTROL_REQUEST_HEADERS']
#
#         h = resp.headers
#
#         # Allow the origin which made the XHR
#         h['Access-Control-Allow-Origin'] = request.headers['Origin']
#         # Allow the actual method
#         h['Access-Control-Allow-Methods'] = request.headers['Access-Control-Request-Method']
#         # Allow for 10 seconds
#         h['Access-Control-Max-Age'] = "10"
#
#         # We also keep current headers
#         if headers is not None:
#             h['Access-Control-Allow-Headers'] = headers
#
#         return resp
#
#
# @app.after_request
# def replace_nan(resp):
#     print resp
#     try:
#         cleaned_data = resp.get_data().replace('nan', 'null').replace('NaN', 'null')
#         resp.set_data(cleaned_data)
#         return resp
#     except:
#         return resp
#
#
# @app.after_request
# def set_allow_origin(resp):
#     """ Set origin for GET, POST, PUT, DELETE requests """
#
#     h = resp.headers
#
#     # Allow crossdomain for other HTTP Verbs
#     if request.method != 'OPTIONS' and 'Origin' in request.headers:
#         h['Access-Control-Allow-Origin'] = request.headers['Origin']
#     return resp
