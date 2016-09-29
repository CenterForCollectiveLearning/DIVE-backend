import yaml
import shutil
import contextlib
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
from dive.worker.ingestion.upload import save_dataset_to_db

excluded_filetypes = ['json', 'py', 'yaml']

app = create_app()
db = SQLAlchemy(app)
manager = Manager(app)
migrate = Migrate(app, db, compare_type=True)
manager.add_command('db', MigrateCommand)

from dive.base.db.models import *

@manager.command
def drop():
    app.logger.info("Dropping tables")
    shutil.rmtree('migrations')
    db.reflect()
    db.drop_all()

@manager.command
def create():
    app.logger.info("Creating tables")

    db.create_all()
    db.session.commit()

@manager.command
def remove_uploads():
    app.logger.info("Removing data directories in upload folder")
    print app.config
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
    with app.app_context():
        user_fixture_file = open('user_fixtures.yaml', 'rt')
        users = yaml.load(user_fixture_file.read())

        for user in users:
            app.logger.info('Created user: %s', user['username'])
            register_user(
                user['username'],
                user['email'],
                user['password'],
                admin=user['admin'],
                teams=user['teams'],
                create_teams=True
            )

@manager.command
def preload():
    preloaded_dir = app.config['PRELOADED_PATH']
    top_level_config_file = open(join(preloaded_dir, 'metadata.yaml'), 'rt')
    top_level_config = yaml.load(top_level_config_file.read())

    # If 'active' flag present, read only those projects. Else iterate through all.
    active_projects = top_level_config.get('active')
    if active_projects:
        project_dirs = active_projects

    # Iterate through project directories
    for project_dir in project_dirs:
        full_project_dir = join(preloaded_dir, project_dir)

        # Validate dir
        if not isdir(full_project_dir) or (project_dir.startswith('.')):
            continue

        # Read config
        project_config = {}
        project_config_file_path = join(full_project_dir, 'metadata.yaml')
        if isfile(project_config_file_path):
            project_config_file = open(project_config_file_path, 'rt')
            project_config = yaml.load(project_config_file.read())
        project_title = project_config.get('title', project_dir)
        project_datasets = project_config.get('datasets')
        private = project_config.get('private', True)

        # Insert projects
        app.logger.info('Preloading project: %s', project_dir)
        with task_app.app_context():
            project_dict = db_access.insert_project(
                title = project_title,
                description = project_config.get('description'),
                preloaded = True,
                topics = project_config.get('topics', []),
                directory = project_dir,
                private = private
            )
            project_id = project_dict['id']

            # Create first document
            db_access.create_document(project_id)


        # Iterate through datasets
        dataset_file_names = listdir(full_project_dir)
        if project_datasets:
            dataset_file_names = project_datasets

        for dataset_file_name in dataset_file_names:
            app.logger.info('Ingesting dataset: %s', dataset_file_name)
            full_dataset_path = join(full_project_dir, dataset_file_name)
            if (not isfile(full_dataset_path)) \
                or dataset_file_name.startswith('.') \
                or (dataset_file_name.rsplit('.')[1] in excluded_filetypes):
                continue

            dataset_title, dataset_type = dataset_file_name.rsplit('.', 1)

            # If dataset-level config for project
            with task_app.app_context():
                datasets = save_dataset(project_id, dataset_title, dataset_file_name, dataset_type, full_dataset_path)

                for dataset in datasets:
                    ingestion_result = ingestion_pipeline.apply(args=[dataset[ 'id'], project_id ])

                    # ingestion_result.get()
        # relationship_result = relationship_pipeline.apply(args=[ project_id ])
        # relationship_result.get()


if __name__ == "__main__":
    manager.run()
