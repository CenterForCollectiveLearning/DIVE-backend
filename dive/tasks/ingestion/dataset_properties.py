'''
Get and compute whole-dataset properties
'''
import pandas as pd

from dive.db import db_access
from dive.task_core import celery, task_app
from dive.data.access import get_data
from dive.data.in_memory_data import InMemoryData as IMD
from dive.tasks.ingestion.type_detection import calculate_field_type, detect_time_series

from celery import states
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


@celery.task(bind=True, track_started=True, name='compute_dataset_properties')
def compute_dataset_properties(self, dataset_id, project_id, path=None):
    ''' Compute and return dictionary containing whole
    import pandas as pd-dataset properties '''
    self.update_state(state=states.PENDING, meta={})

    if not path:
        with task_app.app_context():
            dataset = db_access.get_dataset(project_id, dataset_id)
            path = dataset['path']
            df = get_data(project_id=project_id, dataset_id=dataset_id)

    n_rows, n_cols = df.shape
    field_names = df.columns.values.tolist()

    field_types = []
    for (i, field_name) in enumerate(df):
        field_values = df[field_name]
        field_type, field_type_scores = calculate_field_type(field_name, field_values)
        field_types.append(field_type)

    time_series = detect_time_series(df)
    structure = 'wide' if time_series else 'long'

    properties = {
        'n_rows': n_rows,
        'n_cols': n_cols,
        'field_names': field_names,
        'field_types': field_types,
        'field_accessors': [ i for i in range(0, n_cols) ],
        'structure': structure,
        'is_time_series': time_series,
    }

    self.update_state(state=states.SUCCESS)
    return properties


@celery.task(bind=True, track_started=True, ignore_result=True, name="save_dataset_properties")
def save_dataset_properties(self, properties, dataset_id, project_id):
    self.update_state(state=states.PENDING)
    with task_app.app_context():
        existing_dataset_properties = db_access.get_dataset_properties(project_id, dataset_id)
    if existing_dataset_properties:
        logger.debug("Updating field property of dataset %s", dataset_id)
        with task_app.app_context():
            dataset_properties = db_access.update_dataset_properties(project_id, dataset_id, **properties)
    else:
        logger.debug("Inserting field property of dataset %s", dataset_id)
        with task_app.app_context():
            dataset_properties = db_access.insert_dataset_properties(project_id, dataset_id, **properties)
    self.update_state(state=states.SUCCESS)
