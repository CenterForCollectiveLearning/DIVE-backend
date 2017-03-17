import os
import sys
import pandas.json as pjson

import boto3
import psycopg2.extras
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_cors import CORS
from flask_compress import Compress
from flask_mail import Mail

from raven.contrib.flask import Sentry
from werkzeug.local import LocalProxy

from dive.base.serialization import pjson_dumps, pjson_loads

# Setup logging config
from setup_logging import setup_logging
setup_logging()

psycopg2.extras.register_default_json(
    loads=pjson.loads
)

class CustomSQLAlchemy(SQLAlchemy):
    def apply_driver_hacks(self, app, info, options):
        options["json_serializer"] = pjson_dumps
        options["json_deserializer"] = pjson_loads
        return super(CustomSQLAlchemy, self).apply_driver_hacks(app, info, options)


# Initialize app-based objects
sentry = Sentry()
db = CustomSQLAlchemy()
login_manager = LoginManager()
cors = CORS()
compress = Compress()
s3_client = None
mail = Mail()

def create_app(**kwargs):
    '''
    Initialize Flask application
    '''
    app = Flask(__name__)

    mode = os.environ.get('MODE', 'DEVELOPMENT')
    app.logger.info('Creating base app in mode: %s', mode)
    if mode == 'DEVELOPMENT':
        app.config.from_object('config.DevelopmentConfig')
    elif mode == 'TESTING':
        app.config.from_object('config.TestingConfig')
    elif mode == 'PRODUCTION':
        app.config.from_object('config.ProductionConfig')
        sentry.init_app(app)

    if app.config.get('COMPRESS', True):
        compress.init_app(app)

    db.init_app(app)
    login_manager.init_app(app)
    mail.init_app(app)

    cors.init_app(app,
        resources=r'/*',
        supports_credentials=True,
        allow_headers='Content-Type'
    )

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db.session.remove()

    if app.config['STORAGE_TYPE'] == 's3':
        global s3_client
        s3_client = boto3.client('s3',
            aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY'],
            region_name=app.config['AWS_REGION']
        )

    if app.config['STORAGE_TYPE'] == 'file':
        ensure_directories(app)
    return app


def ensure_directories(app):
    if not os.path.isdir(app.config['STORAGE_PATH']):
        app.logger.info("Creating Upload directory")
        os.mkdir(app.config['STORAGE_PATH'])
