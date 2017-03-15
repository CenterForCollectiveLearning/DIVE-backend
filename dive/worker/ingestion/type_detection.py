import re
import pandas as pd
from csvkit import sniffer
from random import sample as random_sample
import dateutil.parser as dparser
from collections import defaultdict

from dive.worker.ingestion.type_classes import IntegerType, StringType, DecimalType, \
    BooleanType, DateType, DateUtilType, MonthType, DayType, CountryCode2Type, CountryCode3Type, \
    CountryNameType, ContinentNameType
from dive.worker.ingestion.constants import DataType, DataTypeWeights

import logging
logger = logging.getLogger(__name__)


FIELD_TYPES = [
    IntegerType, StringType, DecimalType, DateType,
    BooleanType, DateUtilType, MonthType, DayType, CountryCode2Type, CountryCode3Type,
    CountryNameType, ContinentNameType
]

header_strings = {
    'is': {
        DataType.YEAR.value: ['y', 'Y', 'year', 'Year', 'YEAR'],
        DataType.MONTH.value: ['m', 'M', 'month', 'Month', 'MONTH'],
        DataType.DAY.value: ['d', 'D', 'day', 'Days', 'DAY'],
        DataType.DATETIME.value: ['date', 'Date', 'DATE', 'time', 'Time', 'TIME', 'datetime', 'Datetime', 'DATETIME']
    },
    'in': {
        DataType.YEAR.value: ['year', 'Year', 'YEAR', 'YOB'],
        DataType.MONTH.value: ['month', 'Month', 'MONTH'],
        DataType.DAY.value: ['day', 'Days', 'DAY'],
        DataType.DATETIME.value: ['date', 'Date', 'DATE', 'time', 'Time', 'TIME', 'datetime', 'Datetime', 'DATETIME']
    },
    'pre': {
        DataType.BOOLEAN.value: ['is']
    },
    'post': {
    }
}

def get_type_scores_from_field_name(field_name):
    type_scores = defaultdict(int)
    type_scores[DataType.STRING.value] = 0  # Default to string

    for datatype, strings in header_strings['is'].iteritems():
        for s in strings:
            if field_name is s:
                type_scores[datatype] += 2000

    for datatype, strings in header_strings['in'].iteritems():
        for s in strings:
            if s in field_name:
                type_scores[datatype] += 1000

    for datatype, strings in header_strings['pre'].iteritems():
        for s in strings:
            if field_name.startswith(s):
                type_scores[datatype] += 1000

    for datatype, strings in header_strings['post'].iteritems():
        for s in strings:
            if field_name.endswith(s):
                type_scores[datatype] += 1000
    return type_scores


def get_type_scores_from_field_values(field_values, field_types):
    type_scores = defaultdict(int)
    type_scores[DataType.STRING.value] = 0  # Default to string

    type_instances = []
    for field_type in field_types:
        for type_instance in field_type.instances():
            type_instances.append(type_instance)

    # Detection from values
    # N_values * N_types iterations (no hierarchical tests)
    for field_value in field_values:
        for type_instance in type_instances:
            if type_instance.test(field_value):
                type_scores[type_instance.name] += type_instance.weight
    return type_scores


def calculate_field_type(field_name, field_values, field_position, num_fields, field_types=FIELD_TYPES, num_samples=100, random=True):
    '''
    For each field, returns highest-scoring field type of first num_samples non-empty
    instances.
    '''
    # # Convert to str and drop NAs for type detection
    field_values = field_values.dropna().apply(unicode)

    num_samples = min(len(field_values), num_samples)
    field_sample = random_sample(field_values, num_samples) if random else field_values[:num_samples]

    type_scores_from_name = get_type_scores_from_field_name(field_name)
    type_scores_from_values = get_type_scores_from_field_values(field_sample, field_types)

    # Combine type score dictionaries
    final_type_scores = defaultdict(int)
    for t, score in type_scores_from_name.iteritems():
        final_type_scores[t] += score
    for t, score in type_scores_from_values.iteritems():
        final_type_scores[t] += score

    # Normalize field scores
    score_tuples = []
    normalized_type_scores = {}
    total_score = sum(final_type_scores.values())
    for type_name, score in final_type_scores.iteritems():
        score_tuples.append((type_name, score))
        normalized_type_scores[type_name] = float(score) / total_score

    final_field_type = max(score_tuples, key=lambda t: t[1])[0]
    return (final_field_type, normalized_type_scores)


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
def detect_time_series(df, field_types):
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
        logger.debug("Not a time series: need at least one field to be a date")
        return False

    # 2b) Require at least two fields to be dates
    start_index = col_header_types.index(True)
    end_index = len(col_header_types) - 1 - col_header_types[::-1].index(True)
    if (end_index - start_index) <= 0:
        logger.debug("Not a time series: need at least two contiguous fields to be dates")
        return False

    # 3) Ensure that the contiguous block are all of the same type and numeric
    col_types_of_dates = [field_types[i] for (i, is_date) in enumerate(col_header_types) if is_date]
    if not (len(set(col_types_of_dates)) == 1):
        logger.debug("Not a time series: need contiguous fields to have the same type")
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
