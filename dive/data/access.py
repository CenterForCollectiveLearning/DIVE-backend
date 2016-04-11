'''
Module for reading datasets given some specifier

TODO Rename either this or access.py to be more descriptive
'''
import locale

import numpy as np
import pandas as pd

from dive.task_core import celery, task_app
from dive.data.in_memory_data import InMemoryData as IMD
from dive.db import db_access
from time import time

import logging
logger = logging.getLogger(__name__)

locale.setlocale(locale.LC_NUMERIC, '')

def get_dataset_sample(dataset_id, project_id, start=0, inc=100):
    logger.info("Getting dataset sample with project_id %s and dataset_id %s", project_id, dataset_id)
    end = start + inc  # Upper bound excluded
    df = get_data(dataset_id=dataset_id, project_id=project_id)
    sample = map(list, df.iloc[start:end].values)

    result = db_access.get_dataset_properties(project_id, dataset_id)
    result['sample'] = sample
    return result


def get_data(project_id=None, dataset_id=None, nrows=None, profile=False):
    '''
    Generally return data in different formats

    TODO Change to get_data_as_dataframe
    TODO fill_na arguments
    '''
    if profile:
        start_time = time()
    if IMD.hasData(dataset_id):
        return IMD.getData(dataset_id)

    if dataset_id and project_id:
        dataset = db_access.get_dataset(project_id, dataset_id)
        path = dataset['path']
        dialect = dataset['dialect']

        field_properties = db_access.get_field_properties(project_id, dataset_id)

        # delim = get_delimiter(path)
        df = pd.read_table(
            path,
            skiprows = dataset['offset'],
            sep = dialect['delimiter'],
            engine = 'c',
            # dtype = field_to_type_mapping,
            escapechar = dialect['escapechar'],
            doublequote = dialect['doublequote'],
            quotechar = dialect['quotechar'],
            parse_dates = True,
            nrows = nrows,
            thousands = ','
        )

        sanitized_df = sanitize_df(df)
        coerced_df = coerce_types(sanitized_df, field_properties)

        IMD.insertData(dataset_id, coerced_df)
    if profile:
        logger.debug('[ACCESS] Getting dataset %s took %.3fs', dataset_id, (time() - start_time))
    return df


fields_to_coerce_to_float = [ 'decimal', 'latitude', 'longitude' ]
fields_to_coerce_to_integer = [ 'year', 'integer']
fields_to_coerce_to_string = [ 'string' ]
fields_to_coerce_to_datetime = [ 'datetime' ]
def coerce_types(df, field_properties):
    decimal_fields = []
    integer_fields = []
    string_fields = []
    datetime_fields = []

    for fp in field_properties:
        name = fp['name']
        data_type = fp['type']
        if data_type in fields_to_coerce_to_float:
            decimal_fields.append(name)
        elif data_type in fields_to_coerce_to_integer:
            integer_fields.append(name)
        elif data_type in fields_to_coerce_to_string:
            string_fields.append(name)
        elif data_type in fields_to_coerce_to_datetime:
            datetime_fields.append(name)

    # Forcing data types
    for decimal_field in decimal_fields:
        df[decimal_field] = pd.to_numeric(df[decimal_field], errors='coerce')

    for integer_field in integer_fields:
        df[integer_field] = pd.to_numeric(df[integer_field], errors='coerce')

    for datetime_field in integer_fields:
        df[integer_field] = pd.to_datetime(df[datetime_field], errors='coerce')

    return df


def sanitize_df(df):
    # General Sanitation
    invalid_chars = [ 'n/a', 'na', 'NA', 'NaN', 'n/\a', '.', '\n', '\r\n' ]
    for invalid_char in invalid_chars:
        df.replace(invalid_char, np.nan)
    return df


def make_safe_string(s):
    invalid_chars = '-_.+^$ '
    if not s.startswith('temp_name_'):
        for invalid_char in invalid_chars:
            s = s.replace(invalid_char, '_')
        s = 'temp_name_' + s
    return s


def _construct_conditional_clause(all_field_properties, field_id, operation, criteria):
    field = next((field for field in all_field_properties if field_id == field['id']), None)
    field_general_type = field['general_type']
    field_name = make_safe_string(field['name'])
    if (field_general_type == 'q') or (field_general_type == 't'):
        query_string = '%s %s %s' % (field_name, operation, criteria)
    else:
        query_string = '%s %s "%s"' % (field_name, operation, criteria)
    return query_string


def get_conditioned_data(project_id, dataset_id, df, conditional_arg):
    '''
    Given a data frame and a conditional dict ({ and: [{field_id, operation,
    criteria}], or: [...]}).

    Return the conditioned data frame in same dimensions as original.

    TODO Turn this into an argument of the get_data function
    '''
    full_conditional = {}

    and_clause_list = conditional_arg.get('and')
    or_clause_list = conditional_arg.get('or')
    if not (and_clause_list or or_clause_list):
        return df

    with task_app.app_context():
        desired_keys = ['general_type', 'name', 'id']
        raw_field_properties = db_access.get_field_properties(project_id, dataset_id)
        all_field_properties = [{ k: field[k] for k in desired_keys } for field in raw_field_properties]

    query_strings = {
        'and': '',
        'or': ''
    }

    orig_cols = df.columns.tolist()
    safe_df = df.rename(columns=make_safe_string)

    if and_clause_list:
        for c in and_clause_list:
            clause = _construct_conditional_clause(all_field_properties, c['field_id'], c['operation'], c['criteria'])
            query_strings['and'] = query_strings['and'] + ' & ' + clause

    if or_clause_list:
        for c in or_clause_list:
            clause = _construct_conditional_clause(all_field_properties, c['field_id'], c['operation'], c['criteria'])
            query_strings['or'] = query_strings['or'] + ' | ' + clause

    query_strings['and'] = query_strings['and'].strip(' & ')
    query_strings['or'] = query_strings['or'].strip(' | ')

    # Concatenate
    final_query_string = ''
    if query_strings['and'] and query_strings['or']:
        final_query_string = '%s | %s' % (query_strings['and'], query_strings['or'])
    elif query_strings['and'] and not query_strings['or']:
        final_query_string = query_strings['and']
    elif query_strings['or'] and not query_strings['and']:
        final_query_string = query_strings['or']

    conditioned_df = safe_df.query(final_query_string)
    conditioned_df.columns = orig_cols

    return conditioned_df
