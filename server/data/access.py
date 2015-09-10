'''
Functions for reading, sampling, and detecting types of datasets

No manipulation or calculation, only description
'''

import os
import re
import xlrd
import codecs
import pandas as pd
import dateutil.parser as dparser
from flask import json
import csv

from . import DataType

from config import config
from werkzeug.utils import secure_filename
from db import MongoInstance as MI

from bson.objectid import ObjectId
from in_memory_data import InMemoryData as IMD


types = {
    'numeric': [
        'integer',
        'float',
        'complex',
        'datetime'
    ],
    'factor': [
        'string',
        'logical',
        'country',
        'continent',
    ]
}


# Return dataset
def get_dataset_structure(path):
    df = get_data(path=path)
    header = df.columns.values
    df = df.fillna('')
    n_rows, n_cols = df.shape
    types = get_column_types(df)
    time_series = detect_time_series(df)
    if time_series:
        structure = 'wide'
    else:
        structure = 'long'

    extension = path.rsplit('.', 1)[1]

    column_attrs = [{'name': header[i], 'type': types[i], 'column_id': i} for i in range(0, n_cols)]

    result = {
        'column_attrs': column_attrs,
        'header': list(header),
        'rows': n_rows,
        'cols': n_cols,
        'filetype': extension,
        'structure': structure,
        'time_series': time_series
    }

    return result

def get_dataset_data(path, start=0, inc=1000):
    end = start + inc  # Upper bound excluded
    df = get_data(path=path)
    df = df.fillna('')
    sample = map(list, df.iloc[start:end].values)

    result = get_dataset_structure(path)
    result['sample'] = sample
    return result


# Dataflow:
# 1. Save file in uploads/pID directory
# 2. Save file location in project data collection
# 3. Return sample
def upload_file(pID, file):
    full_file_name = secure_filename(file.filename)
    file_name, file_type = full_file_name.rsplit('.', 1)
    path = os.path.join(config['UPLOAD_FOLDER'], pID, full_file_name)

    datasets = []
    # Flat files
    if file_type in ['csv', 'tsv', 'txt'] :
        file.save(path)

        dID = MI.insertDataset(pID, path, full_file_name)
        data_doc = get_dataset_structure(path)
        data_doc.update({
            'title' : file_name,
            'filename' : full_file_name,
            'dID' : dID,
        })
        datasets.append(data_doc)

    # Excel files
    elif file_type.startswith('xls') :
        file.save(path)

        book = xlrd.open_workbook(path)
        sheet_names = book.sheet_names()

        for sheet_name in sheet_names:
            sheet = book.sheet_by_name(sheet_name)

            # Don't save empty sheets
            if sheet.nrows == 0:
                continue

            csv_file_name = file_name + "_" + sheet_name + ".csv"
            csv_path = os.path.join(config['UPLOAD_FOLDER'], pID, csv_file_name)

            csv_file = open(csv_path, 'wb')
            wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
            for rn in xrange(sheet.nrows) :
                wr.writerow([ unicode(v).encode('utf-8') for v in sheet.row_values(rn) ])
            csv_file.close()

            dID = MI.insertDataset(pID, csv_path, csv_file_name)
            data_doc = get_dataset_structure(csv_path)
            data_doc.update({
                'title' : csv_file_name.rsplit('.', 1)[0],
                'filename' : csv_file_name,
                'dID' : dID
            })

            datasets.append(data_doc)

    elif file_type == 'json' :

        print "Saving file: ", filename
        file.save(path)
        print "Saved file: ", filename

        f = open(path, 'rU')
        json_data = json.load(f)

        path2 = path + ".csv"
        filename2 = filename + ".csv"

        csv_file = open(path2, 'wb')
        wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

        header = json_data[0].keys()

        wr.writerow([v.encode('utf-8') for v in header])

        for i in range(len(json_data)) :
            row = []
            for field in header :
                row.append(json_data[i][field])
            wr.writerow([unicode(v).encode('utf-8') for v in row])
        csv_file.close()

        dID = MI.insertDataset(pID, path2, filename2)

        result = get_dataset_structure(path2)
        result.update({
            'title' : filename2.rsplit('.', 1)[0],
            'filename' : filename2,
            'dID' : dID,
        })
        datasets.append(result)
    return datasets


def get_data(pID=None, dID=None, path=None, nrows=None):
    if IMD.hasData(dID):
        return IMD.getData(dID)
    if path:
        delim = get_delimiter(path)
        df = pd.read_table(path, sep=delim, error_bad_lines=False, nrows=nrows)
    if dID:
        dataset = MI.getData({'_id' : ObjectId(dID)}, pID)[0]
        path = dataset['path']
        delim = get_delimiter(path)
        df = pd.read_table(path, sep=delim, error_bad_lines=False, nrows=nrows)
        IMD.insertData(dID, df)
    return df


INT_REGEX = "^-?[0-9]+$"
# FLOAT_REGEX = "[+-]?(\d+(\.\d*)|\.\d+)([eE][+-]?\d+)?"
#"(\d+(?:[.,]\d*)?)"
FLOAT_REGEX = "^\d+([\,]\d+)*([\.]\d+)?$"


COUNTRY_CODES_2 = ['AD', 'AE', 'AF', 'AG', 'AL', 'AM', 'AO', 'AR', 'AT', 'AU', 'AW', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BM', 'BN', 'BO', 'BR', 'BT', 'BW', 'BY', 'CA', 'CD', 'CF', 'CG', 'CH', 'CI', 'CL', 'CM', 'CN', 'CO', 'CR', 'CU', 'CV', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DO', 'DZ', 'EC', 'EE', 'EG', 'ER', 'ES', 'ET', 'FI', 'FM', 'FO', 'FR', 'GA', 'GB', 'GE', 'GH', 'GI', 'GL', 'GM', 'GN', 'GQ', 'GR', 'GT', 'GW', 'GY', 'HK', 'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IQ', 'IR', 'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KN', 'KP', 'KR', 'KW', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS', 'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MG', 'MK', 'ML', 'MM', 'MN', 'MR', 'MT', 'MU', 'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NE', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR', 'NZ', 'OM', 'PA', 'PE', 'PH', 'PK', 'PL', 'PR', 'PS', 'PT', 'PY', 'QA', 'RO', 'RS', 'RU', 'RW', 'SA', 'SC', 'SD', 'SE', 'SG', 'SI', 'SK', 'SL', 'SN', 'SO', 'SR', 'SS', 'ST', 'SV', 'SY', 'SZ', 'TD', 'TG', 'TH', 'TJ', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TW', 'TZ', 'UA', 'UG', 'UNK', 'US', 'UY', 'UZ', 'VE', 'VI', 'VN', 'VU', 'WS', 'XK', 'YE', 'ZA', 'ZM', 'ZW']
COUNTRY_CODES_3 = ['AND', 'ARE', 'AFG', 'ATG', 'ALB', 'ARM', 'AGO', 'ARG', 'AUT', 'AUS', 'ABW', 'AZE', 'BIH', 'BRB', 'BGD', 'BEL', 'BFA', 'BGR', 'BHR', 'BDI', 'BEN', 'BMU', 'BRN', 'BOL', 'BRA', 'BTN', 'BWA', 'BLR', 'CAN', 'COD', 'CAF', 'COG', 'CHE', 'CIV', 'CHL', 'CMR', 'CHN', 'COL', 'CRI', 'CUB', 'CPV', 'CYP', 'CZE', 'DEU', 'DJI', 'DNK', 'DOM', 'DZA', 'ECU', 'EST', 'EGY', 'ERI', 'ESP', 'ETH', 'FIN', 'FSM', 'FRO', 'FRA', 'GAB', 'GBR', 'GEO', 'GHA', 'GIB', 'GRL', 'GMB', 'GIN', 'GNQ', 'GRC', 'GTM', 'GNB', 'GUY', 'HKG', 'HND', 'HRV', 'HTI', 'HUN', 'IDN', 'IRL', 'ISR', 'IMN', 'IND', 'IRQ', 'IRN', 'ISL', 'ITA', 'JEY', 'JAM', 'JOR', 'JPN', 'KEN', 'KGZ', 'KHM', 'KIR', 'KNA', 'PRK', 'KOR', 'KWT', 'KAZ', 'LAO', 'LBN', 'LCA', 'LIE', 'LKA', 'LBR', 'LSO', 'LTU', 'LUX', 'LVA', 'LBY', 'MAR', 'MCO', 'MDA', 'MNE', 'MDG', 'MKD', 'MLI', 'MMR', 'MNG', 'MRT', 'MLT', 'MUS', 'MDV', 'MWI', 'MEX', 'MYS', 'MOZ', 'NAM', 'NER', 'NGA', 'NIC', 'NLD', 'NOR', 'NPL', 'NRU', 'NZL', 'OMN', 'PAN', 'PER', 'PHL', 'PAK', 'POL', 'PRI', 'PSE', 'PRT', 'PRY', 'QAT', 'ROU', 'SRB', 'RUS', 'RWA', 'SAU', 'SYC', 'SDN', 'SWE', 'SGP', 'SVN', 'SVK', 'SLE', 'SEN', 'SOM', 'SUR', 'SSD', 'STP', 'SLV', 'SYR', 'SWZ', 'TCD', 'TGO', 'THA', 'TJK', 'TLS', 'TKM', 'TUN', 'TON', 'TUR', 'TTO', 'TWN', 'TZA', 'UKR', 'UGA', 'UNK', 'USA', 'URY', 'UZB', 'VEN', 'VIR', 'VNM', 'VUT', 'WSM', 'SCG', 'YEM', 'ZAF', 'ZMB', 'ZWE']
COUNTRY_NAMES = ['Andorra', 'United Arab Emirates', 'Afghanistan', 'Antigua and Barbuda', 'Albania', 'Armenia', 'Angola', 'Argentina', 'Austria', 'Australia', 'Aruba', 'Azerbaijan', 'Bosnia and Herzegovina', 'Barbados', 'Bangladesh', 'Belgium', 'Burkina Faso', 'Bulgaria', 'Bahrain', 'Burundi', 'Benin', 'Bermuda', 'Brunei', 'Bolivia', 'Brazil', 'Bhutan', 'Botswana', 'Belarus', 'Canada', 'Democratic Republic of Congo', 'Central African Republic', 'Congo [Republic]', 'Switzerland', 'Chile', 'Cameroon', 'China', 'Colombia', 'Costa Rica', 'Cuba', 'Cape Verde', 'Cyprus', 'Czech Republic', 'Germany', 'Djibouti', 'Denmark', 'Dominican Republic', 'Algeria', 'Ecuador', 'Estonia', 'Egypt', 'Eritrea', 'Spain', 'Ethiopia', 'Finland', 'Micronesia', 'Faroe Islands', 'France', 'Gabon', 'United Kingdom', 'Georgia', 'Ghana', 'Gibraltar', 'Greenland', 'The Gambia', 'Guinea', 'Equatorial Guinea', 'Greece', 'Guatemala', 'Guinea-Bissau', 'Guyana', 'Hong Kong', 'Honduras', 'Croatia', 'Haiti', 'Hungary', 'Indonesia', 'Ireland', 'Israel', 'Isle of Man', 'India', 'Iraq', 'Iran', 'Iceland', 'Italy', 'Jersey', 'Jamaica', 'Jordan', 'Japan', 'Kenya', 'Kyrgyzstan', 'Cambodia', 'Kiribati', 'Saint Kitts and Nevis', 'North Korea', 'South Korea', 'Kuwait', 'Kazakhstan', 'Laos', 'Lebanon', 'St. Lucia', 'Liechtenstein', 'Sri Lanka', 'Liberia', 'Lesotho', 'Lithuania', 'Luxembourg', 'Latvia', 'Libya', 'Morocco', 'Monaco', 'Moldova', 'Montenegro', 'Madagascar', 'Republic of Macedonia', 'Mali', 'Myanmar [Burma]', 'Mongolia', 'Mauritania', 'Malta', 'Mauritius', 'Maldives', 'Malawi', 'Mexico', 'Malaysia', 'Mozambique', 'Namibia', 'Niger', 'Nigeria', 'Nicaragua', 'Netherlands', 'Norway', 'Nepal', 'Nauru', 'New Zealand', 'Oman', 'Panama', 'Peru', 'Philippines', 'Pakistan', 'Poland', 'Puerto Rico', 'Palestine', 'Portugal', 'Paraguay', 'Qatar', 'Romania', 'Serbia', 'Russia', 'Rwanda', 'Saudi Arabia', 'Seychelles', 'Sudan', 'Sweden', 'Singapore', 'Slovenia', 'Slovakia', 'Sierra Leone', 'Senegal', 'Somalia', 'Suriname', 'South Sudan', 'El Salvador', 'Syria', 'Swaziland', 'Chad', 'Togo', 'Thailand', 'Tajikistan', 'Timor-Leste', 'Turkmenistan', 'Tunisia', 'Tonga', 'Turkey', 'Trinidad and Tobago', 'Taiwan', 'Tanzania', 'Ukraine', 'Uganda', 'Unknown', 'United States', 'Uruguay', 'Uzbekistan', 'Venezuela', 'U.S. Virgin Islands', 'Vietnam', 'Vanuatu', 'Samoa', 'Kosovo', 'Yemen', 'South Africa', 'Zambia', 'Zimbabwe']
CONTINENT_NAMES = ['Asia', 'Europe', 'North America', 'South America', 'Australia', 'Antarctica', 'Africa']

# Utility function to get the type of a variable
# TODO: Parse dates
# TODO: Write algorithm to get best estimate given a sample, not a single variable
def get_variable_type(v):
    v = str(v)

    # Numeric
    if re.match(INT_REGEX, v):
        return DataType.INTEGER.value
    elif re.match(FLOAT_REGEX, v):
        return DataType.FLOAT.value

    # Factors
    else:
        if (v in COUNTRY_CODES_2): return DataType.COUNTRY_CODE_2.value
        elif (v in COUNTRY_CODES_3): return DataType.COUNTRY_CODE_3.value
        elif (v in COUNTRY_NAMES): return DataType.COUNTRY_NAME.value
        elif v in CONTINENT_NAMES: return DataType.CONTINENT_NAME.value
        else:
            r = DataType.STRING.value

        try:
            if dparser.parse(v):
                return DataType.DATETIME.value
        except:
            pass
    return r


# Utility function to detect extension and return delimiter
def get_delimiter(path):
    filename = path.rsplit('/')[-1]
    extension = filename.rsplit('.', 1)[1]
    if extension == 'csv':
        delim = ','
    elif extension == 'tsv':
        delim = '\t'
    # TODO Detect separators intelligently
    elif extension == 'txt':
        delim = ','
    return delim


def is_numeric(x):
    if x in [ DateType.INTEGER.value, DataType.FLOAT.value, DataType.DATETIME.value ]: return True
    else: return False



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


def get_column_types(df):
    '''
    For each column, returns most frequent column type of first 100 non-empty
    instances.

    Args: dataframe
    Returns: list of types
    '''
    print "Getting column types"
    nonempty_col_samples = get_first_n_nonempty_values(df, n=100)

    col_types = []
    for col_samples in nonempty_col_samples:
        if col_samples:
            types = [ get_variable_type(ele) for ele in col_samples]
            most_common = max(map(lambda val: (types.count(val), val), set(types)))[1]
            col_types.append(most_common)
        else:
            col_types.append(DataType.STRING.value)

    print "types:", col_types
    return col_types



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
            if dparser.parse(col):
                date_headers.append(col)
                col_header_types.append(True)
        except ValueError:
            col_header_types.append(False)

    # 2) Find contiguous blocks of headers that are dates
    # 2a) Require at least one field to be a date (to avoid error catching below)
    if not any(col_header_types):
        print "Not a time series: need at least one field to be a date"
        return False

    # 2b) Require at least two fields to be dates
    start_index = col_header_types.index(True)
    end_index = len(col_header_types) - 1 - col_header_types[::-1].index(True)
    if (end_index - start_index) <= 0:
        print "Not a time series: need at least two contiguous fields to be dates"
        return False

    # 3) Ensure that the contiguous block are all of the same type and numeric
    col_types = get_column_types(df)
    col_types_of_dates = [col_types[i] for (i, is_date) in enumerate(col_header_types) if is_date]
    if not (len(set(col_types_of_dates)) == 1):
        print "Not a time series: need contiguous fields to have the same type"
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
