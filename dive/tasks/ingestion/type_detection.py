import re
import pandas as pd
from csvkit import sniffer
import dateutil.parser as dparser

from collections import defaultdict
from dive.tasks.ingestion.type_classes import IntegerType, StringType, DecimalType, \
    BooleanType, DateType, DateUtilType, MonthType, DayType, CountryCode2Type, CountryCode3Type, \
    CountryNameType, ContinentNameType
from dive.tasks.ingestion import DataType, DataTypeWeights

import logging
logger = logging.getLogger(__name__)


FIELD_TYPES = [
    IntegerType, StringType, DecimalType,
    BooleanType, DateUtilType, MonthType, DayType, CountryCode2Type, CountryCode3Type,
    CountryNameType, ContinentNameType
]

def get_field_types(df, types=FIELD_TYPES, num_samples=20):
    '''
    For each field, returns highest-scoring field type of first 100 non-empty
    instances.

    Args: dataframe
    Returns: list of types

    all_fields_type_scores = [
        {bool: 80, str: 20},
        {float: 30, int: 10},
    ]
    '''
    all_fields_type_scores = []
    logger.info("Detecting column types")
    nonempty_field_samples = get_first_n_nonempty_values(df, num_samples)

    type_instances = [i for t in types for i in t.instances()]

    header_strings = {
        DataType.YEAR.value: ['year', 'Year'],
        DataType.MONTH.value: ['month', 'Month'],
        DataType.DAY.value: ['day', 'Days'],
        DataType.BOOLEAN.value: ['is'],
        DataType.DATETIME.value: ['date', 'Date', 'time', 'Time']
    }

    # Tabulate field scores
    for i, field_sample in enumerate(nonempty_field_samples):
        field_type_scores = defaultdict(int)
        # Default to string
        field_type_scores[StringType().name] = 0

        # Detection from field names
        field_name = df.columns[i]
        for datatype, strings in header_strings.iteritems():
            for s in strings:
                if s in field_name:
                    field_type_scores[datatype] += 20

        # Detection from values
        for field_value in field_sample:
            for type_instance in type_instances:
                if type_instance.test(field_value):
                    field_type_scores[type_instance.name] += type_instance.weight
        all_fields_type_scores.append(field_type_scores)
    logger.info(all_fields_type_scores)

    field_types = []
    all_fields_normalized_type_scores = []

    # Normalize field scores
    for field_type_scores in all_fields_type_scores:
        field_normalized_type_scores = {}

        _normalized_score_tuples = []
        total_score = sum(field_type_scores.values())

        for type_name, score in field_type_scores.iteritems():
            normalized_score = float(score) / total_score
            field_normalized_type_scores[type_name] = normalized_score
            _normalized_score_tuples.append((type_name, normalized_score))

        all_fields_normalized_type_scores.append(field_normalized_type_scores)
        field_types.append(max(_normalized_score_tuples, key=lambda t: t[1])[0])

    return (field_types, all_fields_normalized_type_scores)


def get_first_n_nonempty_values(df, n=100):
    '''
    Given a dataframe, return first n non-empty and non-null values for each
    column.
    '''
    result = []
    n = min(df.size, n)

    for col_label in df.columns:
        col = df[col_label]

        i = 0
        max_n = len(col)
        first_n = []
        while ((len(first_n) < n) and (i != len(col) - 1)):
            ele = col[i]
            if (ele != '' and not pd.isnull(ele)):
                first_n.append(ele)
            i = i + 1

        result.append(first_n)
    return result


def detect_if_list(v):
    '''
    Detects list using csvkit sniffer to detect delimiter, splitting on delim
    and filtering common delims to see final list length.

    Returns either a list or False
    '''
    delimiters = ['|', ',', ';', '$']

    LIST_LEN_THRESHOLD = 2
    dialect = sniffer.sniff_dialect(v)
    if dialect:
        delim = dialect.delimiter
        split_vals = v.split(delim)
        filtered_vals = [ x for x in split_vals if (x and (x not in delimiters)) ]
        if filtered_vals >= 2:
            return filtered_vals
    return False


# TODO: Get total range, separation of each data point
##########
# Given a data frame, if a time series is detected then return the start and end indexes
# Else, return False
##########
def detect_time_series(df):
    # 1) Check if any headers are dates
    date_headers = []
    col_header_types = []
    for col in df.columns.values:
        try:
            dparser.parse(col)
            date_headers.append(col)
            col_header_types.append(True)
        except (ValueError, TypeError):
            col_header_types.append(False)

    # 2) Find contiguous blocks of headers that are dates
    # 2a) Require at least one field to be a date (to avoid error catching below)
    if not any(col_header_types):
        logger.info("Not a time series: need at least one field to be a date")
        return False

    # 2b) Require at least two fields to be dates
    start_index = col_header_types.index(True)
    end_index = len(col_header_types) - 1 - col_header_types[::-1].index(True)
    if (end_index - start_index) <= 0:
        logger.info("Not a time series: need at least two contiguous fields to be dates")
        return False

    # 3) Ensure that the contiguous block are all of the same type and numeric
    col_types = get_field_types(df)
    col_types_of_dates = [col_types[i] for (i, is_date) in enumerate(col_header_types) if is_date]
    if not (len(set(col_types_of_dates)) == 1):
        logger.info("Not a time series: need contiguous fields to have the same type")
        return False

    start_name = df.columns.values[start_index]
    end_name = df.columns.values[end_index]
    ts_length = dparser.parse(end_name) - dparser.parse(start_name)
    ts_num_elements = end_index - start_index + 1

    # ASSUMPTION: Uniform intervals in time series
    ts_interval = (dparser.parse(end_name) - dparser.parse(start_name)) / ts_num_elements

    result = {
        'start': {
            'index': start_index,
            'name': start_name
        },
        'end': {
            'index': end_index,
            'name': end_name
        },
        'time_series': {
            'num_elements': end_index - start_index + 1,
            'length': ts_length.total_seconds(),
            'names': date_headers
        }
    }
    return result
