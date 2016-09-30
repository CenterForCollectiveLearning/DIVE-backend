import os
import sys
import pandas.json as pjson

import boto3
import psycopg2.extras
from flask import Flask, request
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import LoginManager
from flask.ext.cors import CORS
from flask.ext.compress import Compress
from raven.contrib.flask import Sentry
from werkzeug.local import LocalProxy

# Setup logging config
from setup_logging import setup_logging
setup_logging()

psycopg2.extras.register_default_json(
    loads=pjson.loads
)

class CustomSQLAlchemy(SQLAlchemy):
    def apply_driver_hacks(self, app, info, options):
        if not "json_serializer" in options:
            options["json_serializer"] = pjson.dumps
        return super(CustomSQLAlchemy, self).apply_driver_hacks(app, info, options)


# Initialize app-based objects
sentry = Sentry()
db = CustomSQLAlchemy()
login_manager = LoginManager()
cors = CORS()
compress = Compress()
s3 = None
s3_bucket = None


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

    cors.init_app(app,
        resources=r'/*',
        supports_credentials=True,
        allow_headers='Content-Type'
    )

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db.session.remove()

    if app.config['STORAGE_TYPE'] == 'file':
        ensure_directories(app)
    return app


def ensure_directories(app):
    if not os.path.isdir(app.config['STORAGE_PATH']):
        app.logger.info("Creating Upload directory")
        os.mkdir(app.config['STORAGE_PATH'])
