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
from dive.worker.serialization import replace_unserializable_numpy

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def run_correlation_from_spec(spec, project_id, conditionals=[]):
    dataset_id = spec.get("datasetId")
    correlation_variables = spec.get("correlationVariables")

    with task_app.app_context():
        df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = get_conditioned_data(project_id, dataset_id, df, conditionals)
    df = df.dropna()  # Remove unclean

    correlation_result = run_correlation(df, correlation_variables)
    return correlation_result, 200


def run_correlation(df, correlation_variables):
    '''
    Runs correlations between pairs of correlation_variables
    df: the dataframe
    correlation_variables: the numerical variables to do the correlation on. A list of names.
    '''
    correlation_result = {}
    correlation_result['headers'] = correlation_variables
    correlation_result['rows'] = []

    data_columns = [ df[correlation_variable] for correlation_variable in correlation_variables ]
    num_variables = len(correlation_variables)

    for row in range(num_variables):
        row_data = []
        for col in range(num_variables):
            if row > col:
                row_data.append([None, None])
            else:
                row_data.append(stats.pearsonr(data_columns[row], data_columns[col]))
        correlation_result['rows'].append({'field': correlation_variables[row], 'data': row_data})

    return correlation_result


def get_correlation_scatterplot_data(correlation_spec, project_id, conditionals=[], max_points=300):
    correlation_variables = correlation_spec['correlationVariables']
    dataset_id = correlation_spec['datasetId']
    with task_app.app_context():
        df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean
    df = get_conditioned_data(project_id, dataset_id, df, conditionals)
    if len(df) > max_points:
        df = df.sample(n=max_points)

    result = []
    for (var_a, var_b) in combinations(correlation_variables, 2):
        data_array = []
        header = [[ var_a, var_b ]]
        for (a, b) in zip(df[var_a], df[var_b]):
            data_array.append([a, b])
        data = header + data_array

        result.append({
            'x': var_a,
            'y': var_b,
            'data': data
        })
    return result


def save_correlation(spec, result, project_id, conditionals={}):
    with task_app.app_context():
        existing_correlation_doc = db_access.get_correlation_from_spec(project_id, spec, conditionals=conditionals)
        if existing_correlation_doc:
            db_access.delete_correlation(project_id, existing_correlation_doc['id'], conditionals=conditionals)
        result = replace_unserializable_numpy(result)
        inserted_correlation = db_access.insert_correlation(project_id, spec, result, conditionals=conditionals)
        return inserted_correlation
