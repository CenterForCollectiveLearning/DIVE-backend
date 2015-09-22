'''
Script to reset development environment:
1) Remove all files in upload directory
2) Drop tables
3) Create clean tables

If tables are dropped, need to run migration script again.
'''
import os
import shutil
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy


def drop_tables(db):
    app.logger.info("Dropping tables")
    db.reflect()
    db.drop_all()

def create_tables(db):
    app.logger.info("Creating tables")
    db.create_all()
    db.session.commit()

def remove_uploads(app):
    app.logger.info("Removing data directories in upload folder")
    if os.path.isdir(app.config['UPLOAD_DIR']):
        UPLOAD_DIR = os.path.join(os.curdir, app.config['UPLOAD_DIR'])
        shutil.rmtree(UPLOAD_DIR)


if __name__ == '__main__':
    app = Flask(__name__)
    app.config.from_object('config.DevelopmentConfig')
    db = SQLAlchemy(app)
    from dive.db.models import Project, Dataset, Field_Properties, Specification, Exported_Specification, Group, User

    remove_uploads(app)
    drop_tables(db)

    # Handled by flask-migrate
    # create_tables(db)
