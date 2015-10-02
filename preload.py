'''
Script to preload projects

TODO Decentralize config file?
TODO Start from config or from directory structure?
TODO Turn this into a celery task?
'''
import yaml
from os import listdir, curdir
from os.path import isfile, join, isdir

from dive.db import db_access
from dive.tasks.pipelines import ingestion_pipeline
from dive.tasks.ingestion.upload import save_dataset


def preload_from_metadata(app):
    '''
    Populate preloaded project tree
    '''
    preloaded_dir = app.config['PRELOADED_DIR']

    config_file = open(join(preloaded_dir, 'metadata.yaml'), 'rt')
    config = yaml.load(config_file.read())

    for topic in config:
        topic_title = topic.get('title')
        topic_dir = topic.get('directory')
        full_topic_dir = join(preloaded_dir, topic_dir)
        if not isdir(full_topic_dir):
            raise NameError, ('%s is not a valid topic directory' % topic_dir)

        for project in topic.get('projects'):
            project_title = project.get('title')
            project_dir = project.get('directory')
            project_desc = project.get('description')
            project_permissions = project.get('permissions')
            full_project_dir = join(full_topic_dir, project_dir)

            if not isdir(full_project_dir):
                raise NameError, ('%s is not a valid project directory' % project_dir)



            for dataset in project.get('datasets'):
                dataset_title = dataset.get('title')
                dataset_file_name = dataset.get('filename')
                dataset_type = dataset_file_name.rsplit('.', 1)[1]

                full_dataset_path = join(full_project_dir, dataset_file_name)

                if not isfile(full_dataset_path):
                    raise NameError, ('%s is not a valid dataset file' % dataset_file_name)

                with app.app_context():
                    dataset_ids = save_dataset(project_id, dataset_title, dataset_file_name, dataset_type, full_dataset_path)

                    for dataset_id in dataset_ids:
                        ingestion_result = ingestion_pipeline(dataset_id, project_id).apply_async()
                        ingestion_result.get()
                print dataset_ids


def preload_from_directory_tree(app):
    preloaded_dir = app.config['PRELOADED_DIR']

    top_level_config_file = open(join(preloaded_dir, 'metadata.yaml'), 'rt')
    top_level_config = yaml.load(top_level_config_file.read())
    active_projects = top_level_config['active']

    for project_dir in listdir(preloaded_dir):
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

        # Only continue on active projects
        if project_title not in active_projects:
            continue

        # Insert projects
        print 'Project:', project_dir
        with app.app_context():
            project_dict = db_access.insert_project(
                title = project_title,
                description = project_config.get('description'),
                preloaded = True,
                topics = project_config.get('topics', []),
                directory = project_dir
            )
        project_id = project_dict['id']


        # Iterate through datasets
        for dataset_file_name in listdir(full_project_dir):
            full_dataset_path = join(full_project_dir, dataset_file_name)
            if not isfile(full_dataset_path) or dataset_file_name.startswith('.') or dataset_file_name.endswith('.yaml'):
                continue

            dataset_title, dataset_type = dataset_file_name.rsplit('.', 1)

            # If dataset-level config for project
            if project_datasets:
                for d in project_datasets:
                    if d['filename'] is dataset_file_name:
                        dataset_title = d.get('title')
                        dataset_description = d.get('description')
                        dataset_type = d.get('description', dataset_type)

            with app.app_context():
                dataset_ids = save_dataset(project_id, dataset_title, dataset_file_name, dataset_type, full_dataset_path)

                for dataset_id in dataset_ids:
                    ingestion_result = ingestion_pipeline(dataset_id, project_id).apply_async()
                    ingestion_result.get()


# TODO Job triggering?
if __name__ == '__main__':
    from dive.core import create_app
    app = create_app()
    preload_from_directory_tree(app)
