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

            with app.app_context():
                project_dict = db_access.insert_project(
                    title = project_title,
                    description = project_desc,
                    preloaded = True,
                    topic = topic_title,
                    directory = join(topic_dir, project_dir)
                )
            project_id = project_dict['id']

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
    # Topic iteration
    for topic_dir in listdir(PRELOADED_DIR):
        full_topic_dir = join(PRELOADED_DIR, topic_dir)
        if isdir(full_topic_dir) and not \
            (topic_dir.startswith('.') or topic_dir.endswith('.yaml')):
            print 'Topic', topic_dir

            # Project iteration
            for project_dir in listdir(full_topic_dir):
                full_project_dir = join(full_topic_dir, project_dir)
                if isdir(full_project_dir) and not (project_dir.startswith('.')):
                    print '\tProject', project_dir

                    # Dataset iteration
                    for dataset in listdir(full_project_dir):
                        if not dataset.startswith('.'):
                            print '\t\tDataset', dataset
    return


# TODO Job triggering?
if __name__ == '__main__':
    from dive.core import create_app
    app = create_app()
    # celery = create_celery(task_app)
    preload_from_metadata(app)
