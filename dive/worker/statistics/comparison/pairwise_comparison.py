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
def get_pairwise_comparison_data(spec, project_id, conditionals={}):
    '''
    For now, spec will be form:
        datasetId
        independentVariables - list names, must be categorical
        dependentVariables - list names, must be numerical
        numBins - number of bins for the independent quantitative variables (if they exist)
    '''
    logger.info('In get_pairwise_comparison_data')
    anova_result = {}

    dependent_variables = spec.get('dependentVariables', [])
    independent_variables = spec.get('independentVariables', [])
    significance_cutoff = spec.get('significanceCutoff', 0.05)
    dataset_id = spec.get('datasetId')

    dependent_variable_names = dependent_variables
    independent_variable_names = [ iv[1] for iv in independent_variables ]

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = get_conditioned_data(project_id, dataset_id, df, conditionals)
    df = df.dropna()  # Remove unclean

    hsd_result = pairwise_tukeyhsd(df[dependent_variable_names[0]], df[independent_variable_names[0]], alpha=significance_cutoff)
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
        'Reject'
    ]
    hsd_data = []
    for i in range(0, len(hsd_raw_data)):
        hsd_data_row = [
            hsd_raw_data[i][0],
            hsd_raw_data[i][1],
            hsd_result.meandiffs[i],
            hsd_result.confint[i][0],
            hsd_result.confint[i][1],
            p_values[i],
            ( 'False' if (p_values[i] <= significance_cutoff) else 'True' )
        ]
        print hsd_data_row
        hsd_data.append(hsd_data_row)

    return {
        'column_headers': hsd_headers,
        'rows': hsd_data
    },200
