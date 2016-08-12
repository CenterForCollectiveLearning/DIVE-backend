'''
Script to preload projects given a directory tree with the following structure:

/PRELOADED_PATH
    metadata.yml (active: [])
    /project
        metadata.yml (OPTIONAL; title: str, description: str, permissions: [], topics: [], source: str, datasets: [])
        dataset

'''
import yaml
from os import listdir, curdir
from os.path import isfile, join, isdir

from dive.base.db import db_access
from dive.worker.core import celery, task_app
from dive.worker.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline, relationship_pipeline
from dive.worker.ingestion.upload import save_dataset


excluded_filetypes = ['json', 'py', 'yaml']


def preload_from_directory_tree(app):
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


if __name__ == '__main__':
    from dive.base.core import create_app
    app = create_app()
    preload_from_directory_tree(app)
