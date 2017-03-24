import pandas as pd
import numpy as np

from dive.base.db import db_access
from dive.base.data.access import get_data, get_conditioned_data
from dive.worker.core import task_app
from dive.worker.ingestion.utilities import get_unique
from dive.worker.ingestion.binning import get_num_bins

from celery.utils.log import get_task_logger
from dive.base.constants import GeneralDataType as GDT
from dive.worker.statistics.aggregation.one_dimensional import create_one_dimensional_contingency_table
from dive.worker.statistics.aggregation.two_dimensional import create_contingency_table
from dive.worker.ingestion.utilities import get_unique

logger = get_task_logger(__name__)


def run_aggregation_from_spec(spec, project_id, config={}, conditionals=[]):
    aggregation_variables_names = spec.get('aggregationVariablesNames')
    dataset_id = spec.get('datasetId')
    dependent_variable_name = spec.get('dependentVariableName')
    weight_variable_name = config.get('weightVariableName')
    num_variables = len(aggregation_variables_names)

    if not (dataset_id): return 'Not passed required parameters', 400

    all_field_properties = db_access.get_field_properties(project_id, dataset_id)
    aggregation_variables = [ next((fp for fp in all_field_properties if fp['name'] == n), None) for n in aggregation_variables_names ]
    dependent_variable = next((fp for fp in all_field_properties if fp['name'] == dependent_variable_name), None)

    subset_variables = aggregation_variables_names
    if dependent_variable_name and dependent_variable_name != 'count':
        subset_variables += [ dependent_variable_name ]
    if weight_variable_name and weight_variable_name != 'UNIFORM':
        subset_variables += [ weight_variable_name ]
    subset_variables = get_unique(subset_variables, preserve_order=True)

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df_conditioned = get_conditioned_data(project_id, dataset_id, df, conditionals)
    df_subset = df_conditioned[ subset_variables ]
    df_ready = df_subset.dropna(how='all')  # Remove unclean

    result = {}
    if num_variables == 1:
        result['one_dimensional_contingency_table'] = create_one_dimensional_contingency_table(df_ready, aggregation_variables[0], dependent_variable, config=config)
    elif num_variables == 2:
        result['two_dimensional_contingency_table'] = create_contingency_table(df_ready, aggregation_variables, dependent_variable, config=config)

    return result, 200


def save_aggregation(spec, result, project_id, config={}, conditionals=[]):
    existing_aggregation_doc = db_access.get_aggregation_from_spec(project_id, spec, config=config, conditionals=conditionals)
    if existing_aggregation_doc:
        db_access.delete_aggregation(project_id, existing_aggregation_doc['id'], config=config, conditionals=conditionals)
    inserted_aggregation = db_access.insert_aggregation(project_id, spec, result, config=config, conditionals=conditionals)
    return inserted_aggregation
