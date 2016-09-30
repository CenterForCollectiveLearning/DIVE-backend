import os
import pandas as pd

from dive.base.db import db_access
from dive.base.data.access import get_data
from dive.worker.core import celery, task_app

from dive.worker.transformation.utilities import list_elements_from_indices, difference_of_lists, get_transformed_file_name


from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def unpivot_dataset(project_id, dataset_id, pivot_fields, variable_name, value_name, new_dataset_name_prefix):
    '''
    Returns unpivoted dataframe
    '''
    with task_app.app_context():
        df = get_data(project_id=project_id, dataset_id=dataset_id)
        project = db_access.get_project(project_id)
        original_dataset = db_access.get_dataset(project_id, dataset_id)

    preloaded_project = project.get('preloaded', False)
    if preloaded_project:
        project_dir = os.path.join(task_app.config['PRELOADED_PATH'], project['directory'])
    else:
        project_dir = os.path.join(task_app.config['STORAGE_PATH'], str(project_id))

    original_dataset_title = original_dataset['title']
    fallback_title = original_dataset_title[:20]
    dataset_type = '.tsv'
    new_dataset_title, new_dataset_name, new_dataset_path = \
        get_transformed_file_name(project_dir, new_dataset_name_prefix, fallback_title, original_dataset_title, dataset_type)

    columns = df.columns.values
    pivot_fields = list_elements_from_indices(columns, pivot_fields)
    preserved_fields = difference_of_lists(columns, pivot_fields)
    df_unpivoted = pd.melt(df, id_vars=preserved_fields, value_vars=pivot_fields, var_name=variable_name, value_name=value_name)

    return df_unpivoted, new_dataset_title, new_dataset_name, new_dataset_path
