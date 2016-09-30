'''
Basic migration script
https://flask-migrate.readthedocs.org/en/latest/
'''
import os
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.script import Manager
from flask.ext.migrate import Migrate, MigrateCommand

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

db = SQLAlchemy(app)
from dive.base.db.models import *
migrate = Migrate(app, db, compare_type=True)

manager = Manager(app)
manager.add_command('db', MigrateCommand)


if __name__ == '__main__':
    manager.run()
