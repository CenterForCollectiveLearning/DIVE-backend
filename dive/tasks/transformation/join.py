import os
import pandas as pd
from flask import current_app

from dive.db import db_access
from dive.data.access import get_data
from dive.task_core import celery, task_app
from dive.tasks.pipelines import ingestion_pipeline
from dive.tasks.ingestion.upload import save_dataset
from dive.tasks.transformation.utilities import list_elements_from_indices, get_transformed_file_name

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def join_datasets(project_id, left_dataset_id, right_dataset_id, on, left_on, right_on, how, left_suffix, right_suffix, new_dataset_name_prefix):
    left_df = get_data(project_id=project_id, dataset_id=left_dataset_id)
    right_df = get_data(project_id=project_id, dataset_id=right_dataset_id)

    with task_app.app_context():
        project = db_access.get_project(project_id)
        original_left_dataset = db_access.get_dataset(project_id, left_dataset_id)
        original_right_dataset = db_access.get_dataset(project_id, right_dataset_id)

    preloaded_project = project.get('preloaded', False)
    if preloaded_project:
        project_dir = os.path.join(current_app.config['PRELOADED_DIR'], project['directory'])
    else:
        project_dir = os.path.join(current_app.config['UPLOAD_DIR'], str(project_id))

    original_left_dataset_title = original_left_dataset['title']
    original_right_dataset_title = original_right_dataset['title']

    fallback_title = original_left_dataset_title[:20] + original_left_dataset_title[:20]
    original_dataset_title = original_left_dataset_title + original_right_dataset_title
    dataset_type = '.tsv'
    new_dataset_title, new_dataset_name, new_dataset_path = \
        get_transformed_file_name(project_dir, new_dataset_name_prefix, fallback_title, original_dataset_title, dataset_type)

    left_columns = left_df.columns.values
    right_columns = right_df.columns.values
    on = list_elements_from_indices(left_columns, on)

    # Not using left_on or right_on for now
    df_joined = left_df.merge(right_df, how=how, on=on, suffixes=[left_suffix, right_suffix])
    df_joined.to_csv(new_dataset_path, sep='\t', index=False)

    dataset_docs = save_dataset(project_id, new_dataset_title, new_dataset_name, 'tsv', new_dataset_path)
    dataset_doc = dataset_docs[0]
    new_dataset_id = dataset_doc['id']

    ingestion_result = ingestion_pipeline(new_dataset_id, project_id).apply()

    return new_dataset_id
