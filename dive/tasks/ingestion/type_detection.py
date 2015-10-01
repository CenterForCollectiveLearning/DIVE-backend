import re
import pandas as pd
from csvkit import sniffer

from collections import defaultdict
from dive.tasks.ingestion.type_classes import IntegerType, StringType, DecimalType, \
    BooleanType, DateType, MonthType, DayType, CountryCode2Type, CountryCode3Type, \
    CountryNameType, ContinentNameType
from dive.tasks.ingestion import DataType, DataTypeWeights

import logging
logger = logging.getLogger(__name__)


def get_field_types(df):
    '''
    For each field, returns highest-scoring field type of first 100 non-empty
    instances.

    Args: dataframe
    Returns: list of types
    '''
    logger.info("Detecting column types")
    nonempty_col_samples = get_first_n_nonempty_values(df, n=100)

    col_types = []
    for samples in nonempty_col_samples:
        if samples:
            # If elements are lists
            aggregate_list = []
            for ele in samples:
                l = detect_if_list(ele)
                if l:
                    aggregate_list.extend(l)
                else:
                    continue

            if aggregate_list:
                types = [ get_variable_type(ele) for ele in aggregate_list ]
                most_common = max(map(lambda val: (types.count(val), val), set(types)))[1]
                col_types.append(most_common)
            else:
                types = [ get_variable_type(ele) for ele in samples]
                most_common = max(map(lambda val: (types.count(val), val), set(types)))[1]
                col_types.append(most_common)
        else:
            col_types.append(DataType.STRING.value)

    return col_types


INT_REGEX = "^-?[0-9]+$"
# FLOAT_REGEX = "[+-]?(\d+(\.\d*)|\.\d+)([eE][+-]?\d+)?" #"(\d+(?:[.,]\d*)?)"
FLOAT_REGEX = "^\d+([\,]\d+)*([\.]\d+)?$"


def get_variable_type(v, strict=False):
    '''
    Detect whether a string represents list of variables or a single variable.
    Then return the most likely type of the variable.
    '''
    return
    # v = str(v)

    # Numeric
    # if re.match(INT_REGEX, v):
    #     return DataType.INTEGER.value
    # elif re.match(FLOAT_REGEX, v):
    #     return DataType.FLOAT.value
    #
    # # Factors
    # else:
    #     if (v in COUNTRY_CODES_2): return DataType.COUNTRY_CODE_2.value
    #     elif (v in COUNTRY_CODES_3): return DataType.COUNTRY_CODE_3.value
    #     elif (v in COUNTRY_NAMES): return DataType.COUNTRY_NAME.value
    #     elif v in CONTINENT_NAMES: return DataType.CONTINENT_NAME.value
    #     elif (v in BOOLEAN_TRUE_VALUES) or (v in BOOLEAN_FALSE_VALUES):
    #         return DataType.BOOLEAN.value
    #     else:
    #         r = DataType.STRING.value
    #
    #     try:
    #         if dparser.parse(v):
    #             return DataType.DATETIME.value
    #     except:
    #         pass
    # return r


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
