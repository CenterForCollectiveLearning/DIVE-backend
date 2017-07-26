import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import ttest_ind
import statsmodels.api as sm
from statsmodels.formula.api import ols
from collections import OrderedDict

from dive.base.db import db_access
from dive.base.serialization import jsonify
from dive.base.data.access import get_data, get_conditioned_data
from dive.worker.ingestion.utilities import get_unique
from dive.worker.visualization.data import get_val_box_data

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def get_anova_boxplot_data(project_id, dataset_id, df, independent_variables_names, dependent_variables_names, NUM_GROUPS_CUTOFF=15):
    anova_result = {}
    considered_independent_variable_name = independent_variables_names[0]
    considered_dependent_variable_name = dependent_variables_names[0]

    # Only return boxplot data if number of groups < THRESHOLD
    num_groups = len(get_unique(df[considered_independent_variable_name]))
    if num_groups > NUM_GROUPS_CUTOFF:
        return None


    val_box_spec = {
        'grouped_field': { 'name': considered_independent_variable_name },
        'boxed_field': { 'name': considered_dependent_variable_name }
    }

    viz_data = get_val_box_data(df, val_box_spec)

    result = {
        'project_id': project_id,
        'dataset_id': dataset_id,
        'spec': val_box_spec,
        'meta': {
            'labels': {
                'x': considered_independent_variable_name,
                'y': considered_dependent_variable_name
            },
        },
        'data': viz_data
    }

    return result
