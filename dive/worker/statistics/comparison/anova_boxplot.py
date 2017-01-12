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


def get_anova_boxplot_data(spec, project_id, conditionals={}):
    anova_result = {}

    dependent_variables = spec.get('dependentVariables', [])
    independent_variables = spec.get('independentVariables', [])
    dependent_variables_names = dependent_variables
    independent_variables_names = [ iv[1] for iv in independent_variables ]
    dataset_id = spec.get('datasetId')

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = get_conditioned_data(project_id, dataset_id, df, conditionals)

    # Only return boxplot data if number of groups < THRESHOLD
    num_groups = len(get_unique(df[independent_variables_names[0]]))
    NUM_GROUP_THRESHOLD = 15
    if num_groups > NUM_GROUP_THRESHOLD:
        return None, 200

    df_subset = df[ dependent_variables_names + independent_variables_names ]
    df_ready = df_subset.dropna(how='all')  # Remove unclean

    val_box_spec = {
        'grouped_field': { 'name': independent_variables[0][1] },
        'boxed_field': { 'name': dependent_variables[0] }
    }

    viz_data = get_val_box_data(df_ready, val_box_spec)

    result = {
        'project_id': project_id,
        'dataset_id': dataset_id,
        'spec': val_box_spec,
        'meta': {
            'labels': {
                'x': independent_variables[0][1],
                'y': dependent_variables[0]
            },
        },
        'data': viz_data
    }

    return result, 200
