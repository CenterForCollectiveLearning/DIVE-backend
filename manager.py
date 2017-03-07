import yaml
import shutil
import contextlib
import os
from os import listdir, curdir
from os.path import isfile, join, isdir
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.migrate import Migrate, MigrateCommand
from flask.ext.script import Manager

from dive.base.core import create_app
from dive.base.db import db_access
from dive.base.db.accounts import register_user
from dive.base.db.constants import Role
from dive.base.db.models import Project, Dataset, Dataset_Properties, Field_Properties, Spec, Exported_Spec, Team, User
from dive.worker.core import celery, task_app
from dive.worker.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline, relationship_pipeline
from dive.worker.ingestion.upload import get_dialect, get_encoding

excluded_filetypes = ['json', 'py', 'yaml']

app = create_app()
app.app_context().push()
mode = os.environ.get('MODE', 'DEVELOPMENT')
if mode == 'DEVELOPMENT': app.config.from_object('config.DevelopmentConfig')
elif mode == 'TESTING': app.config.from_object('config.TestingConfig')
elif mode == 'PRODUCTION': app.config.from_object('config.ProductionConfig')

db = SQLAlchemy(app)
manager = Manager(app)

from dive.base.db.models import *
migrate = Migrate(app, db, compare_type=True)

manager.add_command('db', MigrateCommand)

@manager.command
def fresh_migrations():
    try:
        shutil.rmtree('migrations')
    except OSError as e:
        pass
    command = 'DROP TABLE IF EXISTS alembic_version;'
    db.engine.execute(command)

@manager.command
def drop():
    app.logger.info("Dropping tables")
    try:
        shutil.rmtree('migrations')
    except OSError as e:
        pass
    db.session.commit()
    db.reflect()
    db.drop_all()

@manager.command
def create():
    app.logger.info("Creating tables")
    db.session.commit()
    db.create_all()
    db.session.commit()

@manager.command
def remove_uploads():
    app.logger.info("Removing data directories in upload folder")
    if os.path.isdir(app.config['STORAGE_PATH']):
        STORAGE_PATH = os.path.join(os.curdir, app.config['STORAGE_PATH'])
        shutil.rmtree(STORAGE_PATH)

@manager.command
def recreate():
    app.logger.info("Recreating tables")
    drop()
    create()

@manager.command
def delete_specs():
    from dive.base.db.models import Spec
    all_specs = Spec.query.all()
    map(db.session.delete, all_specs)
    db.session.commit()

@manager.command
def users():
    user_fixture_file = open('fixtures.yaml', 'rt')
    users = yaml.load(user_fixture_file.read())['users']

    for user in users:
        app.logger.info('Created user: %s', user['username'])
        register_user(
            user['username'],
            user['email'],
            user['password'],
            admin=user['admin'],
            teams=user['teams'],
            confirmed=True,
            create_teams=True
        )

@manager.command
def preload():
    preloaded_dir = app.config['PRELOADED_PATH']
    top_level_config_file = open(join(preloaded_dir, 'metadata.yaml'), 'rt')
    top_level_config = yaml.load(top_level_config_file.read())
    dataset_configs = top_level_config['datasets']

    # Iterate through project directories
    for dataset_config in dataset_configs:
        title = dataset_config['title']
        file_name = dataset_config['file']
        description = dataset_config['description']
        synthetic = dataset_config['synthetic']
        topics = dataset_config['topics']
        path = join(preloaded_dir, file_name)

        app.logger.info('Ingesting dataset: %s', file_name)
        _, file_type = file_name.rsplit('.', 1)

        file_object = open(path, 'r')
        project_id = -1

        dialect = get_dialect(file_object)
        encoding = get_encoding(file_object)

        dataset = db_access.insert_dataset(
            project_id,
            path = path,
            encoding = encoding,
            dialect = dialect,
            offset = None,
            title = title,
            file_name = file_name,
            type = file_type,
            preloaded = True,
            storage_type = 'file'
        )

        ingestion_result = ingestion_pipeline.apply(args=[dataset['id'], project_id])
        print ingestion_result.get()
        # relationship_result = relationship_pipeline.apply(args=[ project_id ])
        # relationship_result.get()


if __name__ == "__main__":
    manager.run()
