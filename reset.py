'''
Script to reset development environment (clean database and upload directories)
'''
import os
import shutil
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy


def drop_tables(db):
    db.reflect()
    db.drop_all()

def create_tables(db):
    import dive.db.models
    db.create_all()

def remove_uploads(app):
    app.logger.info("Removing data directories in upload folder")
    if os.path.isdir(app.config['UPLOAD_FOLDER']):
        UPLOAD_FOLDER = os.path.join(os.curdir, app.config['UPLOAD_FOLDER'])
        shutil.rmtree(UPLOAD_FOLDER)


if __name__ == '__main__':
    app = Flask(__name__)
    app.config.from_object('config.DevelopmentConfig')
    db = SQLAlchemy(app)

    remove_uploads(app)
    drop_tables(db)
    create_tables(db)
