'''
Containers for celery task chains
'''
from celery import signature, chain
from dive.task_core import celery, task_app
from dive.tasks.ingestion.dataset_properties import compute_dataset_properties
from dive.tasks.ingestion.field_properties import compute_field_properties
from dive.tasks.visualization.specs import compute_viz_specs

import logging
logger = logging.getLogger(__name__)

def upload_pipeline(dataset_id, project_id):
    logger.info("In upload pipeline")
    result = chain(
        compute_dataset_properties.si(dataset_id, project_id),
        compute_field_properties.si(project_id, dataset_id),
        compute_viz_specs.si(project_id, dataset_id)
    )()
    logger.info("Result %s", result)
    return result
