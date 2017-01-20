'''
Module for reading datasets given some specifier

TODO Rename either this or access.py to be more descriptive
'''
import locale

import os
from time import time
import numpy as np
import pandas as pd
from flask import current_app
from flask_restful import abort

from dive.base.core import s3_client
from dive.base.data.in_memory_data import InMemoryData as IMD
from dive.base.db import db_access
from dive.worker.core import task_app


import logging
logger = logging.getLogger(__name__)

locale.setlocale(locale.LC_NUMERIC, '')


def delete_dataset(project_id, dataset_id):
    deleted_dataset = db_access.delete_dataset(project_id, dataset_id)
    if deleted_dataset['storage_type'] == 's3':
        file_obj = s3_client.delete_object(
            Bucket=current_app.config['AWS_DATA_BUCKET'],
            Key="%s/%s" % (project_id, deleted_dataset['file_name'])
        )
    elif deleted_dataset['storage_type'] == 'file':
        os.remove(deleted_dataset['path'])
    return deleted_dataset


def get_dataset_sample(dataset_id, project_id, start=0, inc=100):
    logger.debug("Getting dataset sample with project_id %s and dataset_id %s", project_id, dataset_id)
    end = start + inc  # Upper bound excluded
    df = get_data(dataset_id=dataset_id, project_id=project_id)
    sample = map(list, df.iloc[start:end].values)

    result = db_access.get_dataset_properties(project_id, dataset_id)
    result['sample'] = sample
    return result


def get_data(project_id=None, dataset_id=None, nrows=None, field_properties=[]):
    if IMD.hasData(dataset_id):
        logger.debug('Accessing from IMD, project_id: %s, dataset_id: %s', project_id, dataset_id)
        df = IMD.getData(dataset_id)
        return df

    logger.debug('Accessing from read, project_id: %s, dataset_id: %s', project_id, dataset_id)

    dataset = db_access.get_dataset(project_id, dataset_id)
    dialect = dataset['dialect']
    encoding = dataset.get('encoding', 'utf-8')

    if dataset['storage_type'] == 's3':
        file_obj = s3_client.get_object(
            Bucket=current_app.config['AWS_DATA_BUCKET'],
            Key="%s/%s" % (project_id, dataset['file_name'])
        )
        accessor = file_obj['Body']
    if dataset['storage_type'] == 'file':
        accessor = dataset['path']

    if not field_properties:
        field_properties = db_access.get_field_properties(project_id, dataset_id)

    df = pd.read_table(
        accessor,
        encoding = encoding,
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
    return coerced_df


fields_to_coerce_to_float = [ 'decimal', 'latitude', 'longitude' ]
fields_to_coerce_to_integer = [ 'year', 'integer' ]
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

    # for datetime_field in datetime_fields:
    #     try:
    #         df[datetime_field] = pd.to_datetime(df[datetime_field], errors='coerce', infer_datetime_format=True)
    #     except ValueError:
    #         df[datetime_field] = pd.to_datetime(df[datetime_field], errors='coerce')

    return df


def sanitize_df(df):
    # General Sanitation
    invalid_chars = [ 'None', '', 'n/a', 'na', 'NA', 'NaN', 'n/\a', '.', '\n', '\r\n' ]
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
