'''
Module for reading datasets given some specifier

TODO Rename either this or access.py to be more descriptive
'''

import pandas as pd

from dive.data.in_memory_data import InMemoryData as IMD
from dive.db import db_access

import logging
logger = logging.getLogger(__name__)


def get_dataset_sample(dataset_id, project_id, start=0, inc=1000):
    logger.info("Getting dataset sample with project_id %s and dataset_id %s", project_id, dataset_id)
    end = start + inc  # Upper bound excluded
    df = get_data(dataset_id=dataset_id, project_id=project_id)
    sample = map(list, df.iloc[start:end].values)

    result = db_access.get_dataset_properties(project_id, dataset_id)
    logger.info("Result, %s", result)
    result['sample'] = sample
    return result


def get_data(project_id=None, dataset_id=None, nrows=None):
    '''
    Generally return data in different formats

    TODO Change to get_data_as_dataframe
    TODO fill_na arguments
    '''
    if IMD.hasData(dataset_id):
        return IMD.getData(dataset_id)

    if dataset_id and project_id:
        dataset = db_access.get_dataset(project_id, dataset_id)
        path = dataset['path']
        dialect = dataset['dialect']

        # delim = get_delimiter(path)
        df = pd.read_table(
            path,
            skiprows = dataset['offset'],
            sep = dialect['delimiter'],
            # lineterminator = dialect['lineterminator'],
            escapechar = dialect['escapechar'],
            doublequote = dialect['doublequote'],
            quotechar = dialect['quotechar'],
            error_bad_lines = False,
            parse_dates = True,
            encoding = 'utf-8',
            nrows = nrows
        )
        df = df.fillna('')
        IMD.insertData(dataset_id, df)
    return df


def get_conditioned_data(df, conditional_arg):
    '''
    Given a data frame and a conditional dict ({ and: [{field, operation,
    criteria}], or: [...]}).

    Return the conditioned data frame in same dimensions as original.

    TODO Turn this into an argument of the get_data function
    '''
    # Replace spaces in column names with underscore
    # cols = df.columns
    # cols = cols.map(lambda x: x.replace(' ', '_') if isinstance(x, (str, unicode)) else x)
    # df.columns = cols
    # print "DF", df.columns
    query_strings = {
        'and': '',
        'or': ''
    }
    orig_cols = df.columns.tolist()
    df.rename(columns=makeSafeString, inplace=True)
    if conditional_arg.get('and'):
        for c in conditional_arg['and']:
            field = makeSafeString(c['field'])
            operation = c['operation']
            criteria = c['criteria']
            criteria_type = get_variable_type(criteria)

            print criteria_type
            if criteria_type in ["integer", "float"]:
                query_string = '%s %s %s' % (field, operation, criteria)
            else:
                query_string = '%s %s "%s"' % (field, operation, criteria)
            query_strings['and'] = query_strings['and'] + ' & ' + query_string

    if conditional_arg.get('or'):
        for c in conditional_arg['or']:
            field = makeSafeString(c['field'])
            operation = c['operation']
            criteria = c['criteria']
            criteria_type = get_variable_type(c['criteria'])

            if criteria_type in ["integer", "float"]:
                query_string = '%s %s %s' % (field, operation, criteria)
            else:
                query_string = '%s %s "%s"' % (field, operation, criteria)
            query_strings['or'] = query_strings['or'] + ' | ' + query_string
    query_strings['and'] = query_strings['and'].strip(' & ')
    query_strings['or'] = query_strings['or'].strip(' | ')

    # Concatenate
    if not (query_strings['and'] or query_strings['or']):
        conditioned_df = df
    else:
        final_query_string = ''
        if query_strings['and'] and query_strings['or']:
            final_query_string = '%s | %s' % (query_strings['and'], query_strings['or'])
        elif query_strings['and'] and not query_strings['or']:
            final_query_string = query_strings['and']
        elif query_strings['or'] and not query_strings['and']:
            final_query_string = query_strings['or']
        print "FINAL_QUERY_STRING:", final_query_string
        conditioned_df = df.query(final_query_string)
    df.columns = orig_cols
    conditioned_df.columns = orig_cols
    return conditioned_df
