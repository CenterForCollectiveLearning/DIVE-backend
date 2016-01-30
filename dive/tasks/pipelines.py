'''
Containers for celery task chains
'''
from celery import group, chain, states
from dive.task_core import celery, task_app
from dive.tasks.ingestion.dataset_properties import compute_dataset_properties, save_dataset_properties
from dive.tasks.ingestion.field_properties import compute_field_properties, save_field_properties
from dive.tasks.ingestion.relationships import compute_relationships, save_relationships
from dive.tasks.visualization.spec_pipeline import attach_data_to_viz_specs, filter_viz_specs, score_viz_specs, save_viz_specs
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


@celery.task(bind=True)
def ingestion_pipeline(self, dataset_id, project_id):
    '''
    Compute dataset and field properties in parallel

    TODO Accept multiple datasets?
    '''
    logger.info("In ingestion pipeline with dataset_id %s and project_id %s", dataset_id, project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(1/4) Computing dataset properties'})
    dataset_properties = compute_dataset_properties(dataset_id, project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(2/4) Saving %s dataset properties'})
    save_dataset_properties(dataset_properties, dataset_id, project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(3/4) Computing dataset field properties'})
    field_properties = compute_field_properties(dataset_id, project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(4/4) Saving dataset field properties'})
    result = save_field_properties(field_properties, dataset_id, project_id)
    return result


@celery.task(bind=True)
def relationship_pipeline(self, project_id):
    logger.info("In relationship modelling pipeline with project_id %s", project_id)
    self.update_state(state=states.PENDING, meta={'desc': '(1/2) Computing relationships'})
    relationships = compute_relationships(project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(2/2) Saving relationships'})
    save_relationships(relationships, project_id)
    return


@celery.task(bind=True)
def viz_spec_pipeline(self, dataset_id, project_id, field_agg_pairs, conditionals):
    '''
    Enumerate, filter, score, and format viz specs in sequence
    '''
    logger.info("In viz spec enumeration pipeline with dataset_id %s and project_id %s", dataset_id, project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(1/5) Enumerating visualization specs'})
    enumerated_viz_specs = enumerate_viz_specs(project_id, dataset_id, field_agg_pairs)

    self.update_state(state=states.PENDING, meta={'desc': '(2/5) Attaching data to %s visualization specs' % len(enumerated_viz_specs)})
    viz_specs_with_data = attach_data_to_viz_specs(enumerated_viz_specs, dataset_id, project_id, conditionals)

    self.update_state(state=states.PENDING, meta={'desc': '(3/5) Filtering %s visualization specs' % len(viz_specs_with_data)})
    filtered_viz_specs = filter_viz_specs(viz_specs_with_data)

    self.update_state(state=states.PENDING, meta={'desc': '(4/5) Scoring %s visualization specs' % len(filtered_viz_specs)})
    scored_viz_specs = score_viz_specs(filtered_viz_specs, dataset_id, project_id, field_agg_pairs)

    self.update_state(state=states.PENDING, meta={'desc': '(5/5) Saving %s visualization specs' % len(scored_viz_specs)})
    saved_viz_specs = save_viz_specs(scored_viz_specs, dataset_id, project_id, field_agg_pairs, conditionals)

    return { 'result': saved_viz_specs }
