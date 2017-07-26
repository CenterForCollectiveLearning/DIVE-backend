import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import ttest_ind
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from statsmodels.stats.libqsturng import psturng
from collections import OrderedDict

from dive.base.db import db_access
from dive.base.data.access import get_data, get_conditioned_data
from dive.worker.ingestion.utilities import get_unique
from dive.worker.statistics.utilities import get_design_matrices, are_variations_equal

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

# http://stackoverflow.com/questions/25791053/anova-and-hsd-tests-from-python-dataframe
def get_pairwise_comparison_data(df, independent_variables_names, dependent_variables_names, significance_cutoff=0.05):
    '''
        datasetId
        independentVariables - list names, must be categorical
        dependentVariables - list names, must be numerical
        numBins - number of bins for the independent quantitative variables (if they exist)
    '''
    considered_independent_variable_name = independent_variables_names[0]
    considered_dependent_variable_name = dependent_variables_names[0]

    # Only return pairwise comparison data if number of groups < THRESHOLD
    num_groups = len(get_unique(df[considered_independent_variable_name]))
    NUM_GROUP_THRESHOLD = 15
    if num_groups > NUM_GROUP_THRESHOLD:
        return None

    hsd_result = pairwise_tukeyhsd(df[considered_dependent_variable_name], df[considered_independent_variable_name], alpha=significance_cutoff)
    hsd_raw_data = hsd_result.summary().data[1:]
    st_range = np.abs(hsd_result.meandiffs) / hsd_result.std_pairs
    p_values = psturng(st_range, len(hsd_result.groupsunique), hsd_result.df_total)

    hsd_headers = [
        'Group 1',
        'Group 2',
        'Group Mean Difference (2 - 1)',
        'Lower Bound',
        'Upper Bound',
        'p-value',
        'Distinct (p < %s)' % significance_cutoff
    ]
    hsd_data = []
    for i in range(0, len(hsd_raw_data)):
        if isinstance(p_values, float):
            p_value = p_values
        else:
            p_value = p_values[i] if i < len(p_values) else None
        hsd_data_row = [
            hsd_raw_data[i][0],
            hsd_raw_data[i][1],
            hsd_result.meandiffs[i],
            hsd_result.confint[i][0],
            hsd_result.confint[i][1],
            p_value,
            ( 'True' if (p_value <= significance_cutoff) else 'False' )
        ]
        hsd_data.append(hsd_data_row)

    return {
        'column_headers': hsd_headers,
        'rows': hsd_data
    }
