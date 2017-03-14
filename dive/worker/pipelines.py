'''
Containers for celery task chains
'''
from celery import group, chain, states
from dive.worker.core import celery, task_app

from dive.worker.handlers import worker_error_handler

from dive.worker.ingestion.upload import save_dataset_to_db
from dive.worker.ingestion.dataset_properties import compute_dataset_properties, save_dataset_properties
from dive.worker.ingestion.field_properties import compute_all_field_properties, save_field_properties
from dive.worker.ingestion.relationships import compute_relationships, save_relationships

from dive.worker.transformation.reduce import reduce_dataset
from dive.worker.transformation.join import join_datasets
from dive.worker.transformation.pivot import unpivot_dataset

from dive.worker.visualization.spec_pipeline import attach_data_to_viz_specs, filter_viz_specs, score_viz_specs, save_viz_specs
from dive.worker.visualization.enumerate_specs import enumerate_viz_specs

from dive.worker.statistics.aggregation import run_aggregation_from_spec, create_one_dimensional_contingency_table_from_spec, create_contingency_table_from_spec, save_aggregation
from dive.worker.statistics.correlation.correlation import run_correlation_from_spec, save_correlation
from dive.worker.statistics.regression.pipelines import run_regression_from_spec, save_regression


import logging
logger = logging.getLogger(__name__)

RETRY_WAIT = 1

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


retry_kwargs = {
    'max_retries': 0,
    'countdown': RETRY_WAIT,
    'link_error': worker_error_handler.s()
}


@celery.task(bind=True)
def reduce_pipeline(self, column_ids_to_keep, new_dataset_name_prefix, dataset_id, project_id):
    logger.info("In reduce pipeline with dataset_id %s and project_id %s", dataset_id, project_id)

    # Unpivot
    self.update_state(state=states.PENDING, meta={'desc': '(1/3) Reducing dataset'})
    df_reduced, new_dataset_title, new_dataset_name, new_dataset_path = \
        reduce_dataset(project_id, dataset_id, column_ids_to_keep, new_dataset_name_prefix)

    # Save
    self.update_state(state=states.PENDING, meta={'desc': '(2/3) Saving reduced dataset'})
    df_reduced.to_csv(new_dataset_path, sep='\t', index=False)
    dataset_docs = save_dataset(project_id, new_dataset_title, new_dataset_name, 'tsv', new_dataset_path)
    dataset_doc = dataset_docs[0]
    new_dataset_id = dataset_doc['id']

    # Ingest
    self.update_state(state=states.PENDING, meta={'desc': '(3/3) Ingesting reduced dataset'})
    ingestion_result = ingestion_pipeline.apply(args=[ new_dataset_id, project_id ])
    return {
        'result': {
            'id': new_dataset_id
        }
    }


@celery.task(bind=True)
def join_pipeline(self, left_dataset_id, right_dataset_id, on, left_on, right_on, how, left_suffix, right_suffix, new_dataset_name_prefix, project_id):
    logger.info("In join pipeline with dataset_ids %s %s and project_id %s", left_dataset_id, right_dataset_id, project_id)

    # Unpivot
    self.update_state(state=states.PENDING, meta={'desc': '(1/3) Joining dataset'})
    df_joined, new_dataset_title, new_dataset_name, new_dataset_path = \
        join_datasets(project_id, left_dataset_id, right_dataset_id, on, left_on, right_on, how, left_suffix, right_suffix, new_dataset_name_prefix)

    # Save
    self.update_state(state=states.PENDING, meta={'desc': '(2/3) Saving joined dataset'})
    df_joined.to_csv(new_dataset_path, sep='\t', index=False)
    dataset_docs = save_dataset(project_id, new_dataset_title, new_dataset_name, 'tsv', new_dataset_path)
    dataset_doc = dataset_docs[0]
    new_dataset_id = dataset_doc['id']

    # Ingest
    self.update_state(state=states.PENDING, meta={'desc': '(3/3) Ingesting joined dataset'})
    ingestion_result = ingestion_pipeline.apply(args=[ new_dataset_id, project_id ])
    return {
        'result': {
            'id': new_dataset_id
        }
    }


@celery.task(bind=True)
def unpivot_pipeline(self, pivot_fields, variable_name, value_name, new_dataset_name_prefix, dataset_id, project_id):
    logger.info("In unpivot pipeline with dataset_id %s and project_id %s", dataset_id, project_id)

    # Unpivot
    self.update_state(state=states.PENDING, meta={'desc': '(1/3) Unpivoting dataset'})
    df_unpivoted, new_dataset_title, new_dataset_name, new_dataset_path = \
        unpivot_dataset(project_id, dataset_id, pivot_fields, variable_name, value_name, new_dataset_name_prefix)

    # Save
    self.update_state(state=states.PENDING, meta={'desc': '(2/3) Saving unpivoted dataset'})
    df_unpivoted.to_csv(new_dataset_path, sep='\t', index=False)
    dataset_docs = save_dataset(project_id, new_dataset_title, new_dataset_name, 'tsv', new_dataset_path)
    dataset_doc = dataset_docs[0]
    new_dataset_id = dataset_doc['id']

    # Ingest
    self.update_state(state=states.PENDING, meta={'desc': '(3/3) Ingesting unpivoted dataset'})
    ingestion_result = ingestion_pipeline.apply(args=[ new_dataset_id, project_id ])
    return {
        'result': {
            'id': new_dataset_id
        }
    }



@celery.task(bind=True)
def relationship_pipeline(self, project_id):
    logger.info("In relationship modelling pipeline with project_id %s", project_id)
    self.update_state(state=states.PENDING, meta={'desc': '(1/2) Computing relationships'})
    relationships = compute_relationships(project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(2/2) Saving relationships'})
    save_relationships(relationships, project_id)
    return


@celery.task(bind=True)  #, autoretry_for=(Exception,), retry_kwargs=retry_kwargs)
def viz_spec_pipeline(self, dataset_id, project_id, field_agg_pairs, recommendation_types, conditionals, config):
    '''
    Enumerate, filter, score, and format viz specs in sequence
    '''
    logger.info("In viz spec enumeration pipeline with dataset_id %s and project_id %s", dataset_id, project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(1/5) Enumerating visualization specs'})
    enumerated_viz_specs = enumerate_viz_specs(project_id, dataset_id, field_agg_pairs, recommendation_types=recommendation_types)

    self.update_state(state=states.PENDING, meta={'desc': '(2/5) Attaching data to %s visualization specs' % len(enumerated_viz_specs)})
    viz_specs_with_data = attach_data_to_viz_specs(enumerated_viz_specs, dataset_id, project_id, conditionals, config, data_formats=['visualize', 'score'])

    self.update_state(state=states.PENDING, meta={'desc': '(3/5) Filtering %s visualization specs' % len(viz_specs_with_data)})
    filtered_viz_specs = filter_viz_specs(viz_specs_with_data)

    self.update_state(state=states.PENDING, meta={'desc': '(4/5) Scoring %s visualization specs' % len(filtered_viz_specs)})
    scored_viz_specs = score_viz_specs(filtered_viz_specs, dataset_id, project_id, field_agg_pairs)

    self.update_state(state=states.PENDING, meta={'desc': '(5/5) Saving %s visualization specs' % len(scored_viz_specs)})
    saved_viz_specs = save_viz_specs(scored_viz_specs, dataset_id, project_id, field_agg_pairs, recommendation_types, conditionals, config)
    return { 'result': saved_viz_specs }


@celery.task(bind=True)  #, autoretry_for=(Exception,), retry_kwargs=retry_kwargs)
def ingestion_pipeline(self, dataset_id, project_id):
    '''
    Compute dataset and field properties in parallel

    TODO Accept multiple datasets?
    '''
    logger.info("In ingestion pipeline with dataset_id %s and project_id %s", dataset_id, project_id)
    self.update_state(state=states.PENDING, meta={'desc': '(1/4) Computing dataset properties'})
    dataset_properties = compute_dataset_properties(dataset_id, project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(2/4) Saving dataset properties'})
    save_dataset_properties(dataset_properties, dataset_id, project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(3/4) Computing dataset field properties'})
    field_properties = compute_all_field_properties(dataset_id, project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(4/4) Saving dataset field properties'})
    result = save_field_properties(field_properties, dataset_id, project_id)
    return result

@celery.task(bind=True)  #, autoretry_for=(Exception,), retry_kwargs=retry_kwargs)
def regression_pipeline(self, spec, project_id, conditionals=[]):
    # try:
        # raise Exception('test')
    logger.info("In regression pipeline with and project_id %s", project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(1/2) Running regressions'})
    regression_data, status = run_regression_from_spec(spec, project_id, conditionals=conditionals)

    self.update_state(state=states.PENDING, meta={'desc': '(2/2) Saving regression results'})
    regression_doc = save_regression(spec, regression_data, project_id, conditionals=conditionals)
    regression_data['id'] = regression_doc['id']

    return { 'result': regression_data }
    # except Exception as e:
    #     print 'Updating state in exception handler', states.FAILURE, e
    #     self.update_state(state=states.FAILURE, meta={'error': e})
    #     return

@celery.task(bind=True)  #, autoretry_for=(Exception,), retry_kwargs=retry_kwargs)
def aggregation_pipeline(self, spec, project_id):
    logger.info("In aggregation pipeline with and project_id %s", project_id, conditionals=[])

    self.update_state(state=states.PENDING, meta={'desc': '(1/2) Calculating statistical aggregation'})
    aggregation_data, status = run_aggregation_from_spec(spec, project_id, conditionals=conditionals)

    self.update_state(state=states.PENDING, meta={'desc': '(2/2) Saving statistical aggregation'})
    aggregation_doc = save_aggregation(spec, aggregation_data, project_id, conditionals=conditionals)
    aggregation_data['id'] = aggregation_doc['id']


@celery.task(bind=True)  #, autoretry_for=(Exception,), retry_kwargs=retry_kwargs)
def one_dimensional_contingency_table_pipeline(self, spec, project_id, conditionals=[]):
    logger.info("In one dimensional contingency table pipeline with and project_id %s", project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(1/2) Calculating one dimensional aggregation table'})
    table_data, status = create_one_dimensional_contingency_table_from_spec(spec, project_id, conditionals=conditionals)

    self.update_state(state=states.PENDING, meta={'desc': '(2/2) Saving one dimensional aggregation table'})
    table_doc = save_aggregation(spec, table_data, project_id, conditionals=conditionals)
    table_data['id'] = table_doc['id']

    return { 'result': table_data }


@celery.task(bind=True)  #, autoretry_for=(Exception,), retry_kwargs=retry_kwargs)
def contingency_table_pipeline(self, spec, project_id, conditionals=[]):
    logger.info("In contingency table pipeline with and project_id %s", project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(1/2) Calculating aggregation table'})
    table_data, status = create_contingency_table_from_spec(spec, project_id, conditionals=conditionals)

    self.update_state(state=states.PENDING, meta={'desc': '(2/2) Saving aggregation table'})
    table_doc = save_aggregation(spec, table_data, project_id, conditionals=conditionals)
    table_data['id'] = table_doc['id']

    return { 'result': table_data }


@celery.task(bind=True)  #, autoretry_for=(Exception,), retry_kwargs=retry_kwargs)
def correlation_pipeline(self, spec, project_id, conditionals=[]):
    logger.info("In correlation pipeline with and project_id %s", project_id)

    self.update_state(state=states.PENDING, meta={'desc': '(1/2) Calculating statistical correlation'})
    correlation_data, status = run_correlation_from_spec(spec, project_id, conditionals=conditionals)

    self.update_state(state=states.PENDING, meta={'desc': '(2/2) Saving statistical correlation'})
    correlation_doc = save_correlation(spec, correlation_data, project_id, conditionals=conditionals)
    correlation_data['id'] = correlation_doc['id']
    return { 'result': correlation_data }
