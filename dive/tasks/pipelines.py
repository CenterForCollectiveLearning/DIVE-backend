'''
Containers for celery task chains
'''
from celery import group, chain
from dive.task_core import celery, task_app
from dive.tasks.ingestion.dataset_properties import compute_dataset_properties, save_dataset_properties
from dive.tasks.ingestion.field_properties import compute_field_properties, save_field_properties
from dive.tasks.ingestion.relationships import compute_relationships, save_relationships
from dive.tasks.visualization.spec_pipeline import attach_data_to_viz_specs, filter_viz_specs, score_viz_specs, format_viz_specs, save_viz_specs
from dive.tasks.visualization.enumerate_specs import enumerate_viz_specs


import logging
logger = logging.getLogger(__name__)


def get_chain_IDs(task):
    parent = task.parent
    if parent:
        return get_chain_IDs(parent) + [ task.id ]
    else:
        return [ task.id ]


def full_pipeline(dataset_id, project_id):
    '''
    Get properties and then get viz specs
    '''
    pipeline = chain([
        ingestion_pipeline(dataset_id, project_id),
        viz_spec_pipeline(dataset_id, project_id, [])
    ])
    return pipeline


def ingestion_pipeline(dataset_id, project_id):
    '''
    Compute dataset and field properties in parallel

    TODO Accept multiple datasets?
    '''
    logger.info("In ingestion pipeline with dataset_id %s and project_id %s", dataset_id, project_id)

    pipeline = chain([
        chain([
            compute_dataset_properties.si(dataset_id, project_id),
            save_dataset_properties.s(dataset_id, project_id)
        ]),
        chain([
            compute_field_properties.si(dataset_id, project_id),
            save_field_properties.s(dataset_id, project_id)
        ]),
    ])
    return pipeline


def relationship_pipeline(project_id):
    logger.info("In relationship modelling pipeline with project_id %s", project_id)
    pipeline = chain([
        compute_relationships.si(project_id),
        save_relationships.s(project_id)
    ])
    return pipeline


def viz_spec_pipeline(dataset_id, project_id, field_agg_pairs, conditionals):
    '''
    Enumerate, filter, score, and format viz specs in sequence
    '''
    logger.info("In viz spec enumeration pipeline with dataset_id %s and project_id %s", dataset_id, project_id)

    pipeline = chain([
        enumerate_viz_specs.si(project_id, dataset_id, field_agg_pairs),
        attach_data_to_viz_specs.s(dataset_id, project_id, conditionals),
        filter_viz_specs.s(project_id),
        score_viz_specs.s(dataset_id, project_id, field_agg_pairs),
        # format_viz_specs.s(project_id),
        save_viz_specs.s(dataset_id, project_id, field_agg_pairs, conditionals)
    ])
    pipeline.trail = True
    return pipeline
