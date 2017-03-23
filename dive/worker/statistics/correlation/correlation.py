import random
import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
from time import time
from itertools import chain, combinations
from operator import add, mul
import time
from math import log10, floor

from scipy.stats import ttest_ind

from dive.base.db import db_access
from dive.base.data.access import get_data, get_conditioned_data
from dive.worker.core import task_app
from dive.worker.ingestion.utilities import get_unique


from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def run_correlation_from_spec(spec, project_id, conditionals=[]):
    dataset_id = spec.get("datasetId")
    correlation_variables = spec.get("correlationVariables")
    correlation_variables_names = correlation_variables

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = get_conditioned_data(project_id, dataset_id, df, conditionals)

    df_subset = df[ correlation_variables_names ]
    df_ready = df_subset.dropna(how='all')

    correlation_result = run_correlation(df_ready, correlation_variables)
    correlation_scatterplots = get_correlation_scatterplot_data(df_ready, correlation_variables)
    return {
        'table': correlation_result,
        'scatterplots': correlation_scatterplots
    }, 200


def run_correlation(df, correlation_variables):
    '''
    Runs correlations between pairs of correlation_variables
    df: the dataframe
    correlation_variables: the numerical variables to do the correlation on. A list of names.
    '''
    correlation_result = {}
    correlation_result['headers'] = correlation_variables
    correlation_result['rows'] = []

    df_subset = df[correlation_variables]

    # data_columns = [ df[correlation_variable] for correlation_variable in correlation_variables ]
    num_variables = len(correlation_variables)

    for row_index, row_name in enumerate(correlation_variables):
        row_data = []
        for col_index, col_name in enumerate(correlation_variables):
            if row_index > col_index:
                row_data.append([None, None])
            elif row_index == col_index:
                row_data.append([1.0, 0.0])
            else:
                df_subset_pair = df_subset[[ row_name, col_name ]]
                df_ready = df_subset_pair.dropna(how='any')
                try:
                    r2 = stats.pearsonr(df_ready[row_name], df_ready[col_name])
                except Exception as e:
                    r2 = 0
                row_data.append(r2)
        correlation_result['rows'].append({'field': row_name, 'data': row_data})

    return correlation_result


def get_correlation_scatterplot_data(df, correlation_variables, max_points=100):
    result = []
    for (var_a, var_b) in combinations(correlation_variables, 2):
        df_subset_pair = df[[var_a, var_b]].dropna(how='any')
        if len(df_subset_pair) > max_points:
            df_subset_pair = df_subset_pair.sample(n=max_points)
        data_array = []
        header = [[ var_a, var_b ]]
        for (a, b) in zip(df_subset_pair[var_a], df_subset_pair[var_b]):
            data_array.append([a, b])
        data = header + data_array

        result.append({
            'x': var_a,
            'y': var_b,
            'data': data
        })
    return result


def save_correlation(spec, result, project_id, conditionals={}):
    existing_correlation_doc = db_access.get_correlation_from_spec(project_id, spec, conditionals=conditionals)
    if existing_correlation_doc:
        db_access.delete_correlation(project_id, existing_correlation_doc['id'], conditionals=conditionals)
    inserted_correlation = db_access.insert_correlation(project_id, spec, result, conditionals=conditionals)
    return inserted_correlation
