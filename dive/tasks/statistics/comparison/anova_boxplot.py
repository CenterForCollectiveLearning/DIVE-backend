import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import ttest_ind
import statsmodels.api as sm
from statsmodels.formula.api import ols
from collections import OrderedDict

from dive.db import db_access
from dive.data.access import get_data
from dive.tasks.ingestion.utilities import get_unique
from dive.tasks.statistics.utilities import get_design_matrices, are_variations_equal

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def get_anova_boxplot_data(spec, project_id):
    '''
    For now, spec will be form:
        datasetId
        independentVariables - list names, must be categorical
        dependentVariables - list names, must be numerical
        numBins - number of bins for the independent quantitative variables (if they exist)
    '''
    anova_result = {}

    dependent_variables = spec.get('dependentVariables', [])
    independent_variables = spec.get('independentVariables', [])
    dataset_id = spec.get('datasetId')

    quantiles = [0.01, 0.25, 0.5, 0.75, 0.99]

    dependent_variable_names = dependent_variables
    independent_variable_names = [ iv[1] for iv in independent_variables ]

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    variable_subset = dependent_variable_names + independent_variable_names
    df_quantiles = df[variable_subset].groupby(independent_variable_names).quantile(quantiles)

    one_way = (len(independent_variable_names) == 1)

    # { grouped_field_value: { quantile: value } }
    results_grouped_by_highest_level = dict()
    for specifier, value in df_quantiles.to_dict()[dependent_variable_names[0]].iteritems():
        if one_way:
            grouped_field_value, quantile_level = specifier
            if grouped_field_value in results_grouped_by_highest_level:
                results_grouped_by_highest_level[grouped_field_value][quantile_level] = value
            else:
                results_grouped_by_highest_level[grouped_field_value] = { quantile_level: value }

    # Sort OrderedDict
    results_grouped_by_highest_level = OrderedDict(results_grouped_by_highest_level)

    final_data_array = [ independent_variable_names + quantiles ]
    for grouped_field_value, quantile_value in results_grouped_by_highest_level.iteritems():
        quantile_value_sorted = OrderedDict(quantile_value)
        data_element = [ grouped_field_value ]
        for quantile in sorted(quantile_value.keys()):
            value = quantile_value[quantile]
            data_element.append(value)
        final_data_array.append(data_element)

    return final_data_array, 200
