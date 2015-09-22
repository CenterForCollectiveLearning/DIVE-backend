'''
Containers for celery task chains
'''
from celery import signature
from dive.tasks import celery
from dive.tasks.ingestion.dataset_properties import compute_dataset_properties
from dive.tasks.ingestion.field_properties import compute_field_properties
from dive.tasks.visualization.specs import compute_viz_specs

import logging
logger = logging.getLogger(__name__)

def full_pipeline(dID):
    logger.info("In full pipeline")
    res = chain(
        upload_file.s(),
        compute_dataset_properties.s(),
        compute_field_properties.s(),
        compute_viz_specs.()
    )
