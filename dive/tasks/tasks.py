'''
Containers for celery task chains
'''
from celery import signature, chain, group
from dive.task_core import celery, task_app
from dive.tasks.ingestion.dataset_properties import compute_dataset_properties
from dive.tasks.ingestion.field_properties import get_field_properties, compute_field_properties
from dive.tasks.visualization.specs import enumerate_viz_specs, filter_viz_specs, score_viz_specs, format_viz_specs

import logging
logger = logging.getLogger(__name__)


def ingestion_pipeline(dataset_id, project_id):
    '''
    Get dataset and field properties in parallel
    '''
    logger.info("In ingestion pipeline with dataset_id %s and project_id %s", dataset_id, project_id)
    # Put data upload in here?

    result = group(
        compute_dataset_properties.si(dataset_id, project_id),  # Chain storage
        compute_field_properties.si(project_id, dataset_id),  # Chain storage
    )()
    logger.info("Result %s", result)
    return result


def viz_spec_pipeline(dataset_id, project_id):
    '''
    Enumerate then score viz specs in sequence
    '''
    logger.info("In viz spec enumeration pipeline with dataset_id %s and project_id %s", dataset_id, project_id)

    result = chain(
        enumerate_viz_specs.s(dataset_id, project_id),
        filter_viz_specs.s(project_id),
        score_viz_specs.s(project_id),
        format_viz_specs.s(project_id)
        # Store storage
    )()
    logger.info("Result %s", result)
    return result
