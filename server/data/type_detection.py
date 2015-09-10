import re
import pandas as pd
import dateutil.parser as dparser
from csvkit import sniffer

from . import DataType

INT_REGEX = "^-?[0-9]+$"
# FLOAT_REGEX = "[+-]?(\d+(\.\d*)|\.\d+)([eE][+-]?\d+)?"
#"(\d+(?:[.,]\d*)?)"
FLOAT_REGEX = "^\d+([\,]\d+)*([\.]\d+)?$"


COUNTRY_CODES_2 = ['AD', 'AE', 'AF', 'AG', 'AL', 'AM', 'AO', 'AR', 'AT', 'AU', 'AW', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BM', 'BN', 'BO', 'BR', 'BT', 'BW', 'BY', 'CA', 'CD', 'CF', 'CG', 'CH', 'CI', 'CL', 'CM', 'CN', 'CO', 'CR', 'CU', 'CV', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DO', 'DZ', 'EC', 'EE', 'EG', 'ER', 'ES', 'ET', 'FI', 'FM', 'FO', 'FR', 'GA', 'GB', 'GE', 'GH', 'GI', 'GL', 'GM', 'GN', 'GQ', 'GR', 'GT', 'GW', 'GY', 'HK', 'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IQ', 'IR', 'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KN', 'KP', 'KR', 'KW', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS', 'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MG', 'MK', 'ML', 'MM', 'MN', 'MR', 'MT', 'MU', 'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NE', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR', 'NZ', 'OM', 'PA', 'PE', 'PH', 'PK', 'PL', 'PR', 'PS', 'PT', 'PY', 'QA', 'RO', 'RS', 'RU', 'RW', 'SA', 'SC', 'SD', 'SE', 'SG', 'SI', 'SK', 'SL', 'SN', 'SO', 'SR', 'SS', 'ST', 'SV', 'SY', 'SZ', 'TD', 'TG', 'TH', 'TJ', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TW', 'TZ', 'UA', 'UG', 'UNK', 'US', 'UY', 'UZ', 'VE', 'VI', 'VN', 'VU', 'WS', 'XK', 'YE', 'ZA', 'ZM', 'ZW']
COUNTRY_CODES_3 = ['AND', 'ARE', 'AFG', 'ATG', 'ALB', 'ARM', 'AGO', 'ARG', 'AUT', 'AUS', 'ABW', 'AZE', 'BIH', 'BRB', 'BGD', 'BEL', 'BFA', 'BGR', 'BHR', 'BDI', 'BEN', 'BMU', 'BRN', 'BOL', 'BRA', 'BTN', 'BWA', 'BLR', 'CAN', 'COD', 'CAF', 'COG', 'CHE', 'CIV', 'CHL', 'CMR', 'CHN', 'COL', 'CRI', 'CUB', 'CPV', 'CYP', 'CZE', 'DEU', 'DJI', 'DNK', 'DOM', 'DZA', 'ECU', 'EST', 'EGY', 'ERI', 'ESP', 'ETH', 'FIN', 'FSM', 'FRO', 'FRA', 'GAB', 'GBR', 'GEO', 'GHA', 'GIB', 'GRL', 'GMB', 'GIN', 'GNQ', 'GRC', 'GTM', 'GNB', 'GUY', 'HKG', 'HND', 'HRV', 'HTI', 'HUN', 'IDN', 'IRL', 'ISR', 'IMN', 'IND', 'IRQ', 'IRN', 'ISL', 'ITA', 'JEY', 'JAM', 'JOR', 'JPN', 'KEN', 'KGZ', 'KHM', 'KIR', 'KNA', 'PRK', 'KOR', 'KWT', 'KAZ', 'LAO', 'LBN', 'LCA', 'LIE', 'LKA', 'LBR', 'LSO', 'LTU', 'LUX', 'LVA', 'LBY', 'MAR', 'MCO', 'MDA', 'MNE', 'MDG', 'MKD', 'MLI', 'MMR', 'MNG', 'MRT', 'MLT', 'MUS', 'MDV', 'MWI', 'MEX', 'MYS', 'MOZ', 'NAM', 'NER', 'NGA', 'NIC', 'NLD', 'NOR', 'NPL', 'NRU', 'NZL', 'OMN', 'PAN', 'PER', 'PHL', 'PAK', 'POL', 'PRI', 'PSE', 'PRT', 'PRY', 'QAT', 'ROU', 'SRB', 'RUS', 'RWA', 'SAU', 'SYC', 'SDN', 'SWE', 'SGP', 'SVN', 'SVK', 'SLE', 'SEN', 'SOM', 'SUR', 'SSD', 'STP', 'SLV', 'SYR', 'SWZ', 'TCD', 'TGO', 'THA', 'TJK', 'TLS', 'TKM', 'TUN', 'TON', 'TUR', 'TTO', 'TWN', 'TZA', 'UKR', 'UGA', 'UNK', 'USA', 'URY', 'UZB', 'VEN', 'VIR', 'VNM', 'VUT', 'WSM', 'SCG', 'YEM', 'ZAF', 'ZMB', 'ZWE']
COUNTRY_NAMES = ['Andorra', 'United Arab Emirates', 'Afghanistan', 'Antigua and Barbuda', 'Albania', 'Armenia', 'Angola', 'Argentina', 'Austria', 'Australia', 'Aruba', 'Azerbaijan', 'Bosnia and Herzegovina', 'Barbados', 'Bangladesh', 'Belgium', 'Burkina Faso', 'Bulgaria', 'Bahrain', 'Burundi', 'Benin', 'Bermuda', 'Brunei', 'Bolivia', 'Brazil', 'Bhutan', 'Botswana', 'Belarus', 'Canada', 'Democratic Republic of Congo', 'Central African Republic', 'Congo [Republic]', 'Switzerland', 'Chile', 'Cameroon', 'China', 'Colombia', 'Costa Rica', 'Cuba', 'Cape Verde', 'Cyprus', 'Czech Republic', 'Germany', 'Djibouti', 'Denmark', 'Dominican Republic', 'Algeria', 'Ecuador', 'Estonia', 'Egypt', 'Eritrea', 'Spain', 'Ethiopia', 'Finland', 'Micronesia', 'Faroe Islands', 'France', 'Gabon', 'United Kingdom', 'Georgia', 'Ghana', 'Gibraltar', 'Greenland', 'The Gambia', 'Guinea', 'Equatorial Guinea', 'Greece', 'Guatemala', 'Guinea-Bissau', 'Guyana', 'Hong Kong', 'Honduras', 'Croatia', 'Haiti', 'Hungary', 'Indonesia', 'Ireland', 'Israel', 'Isle of Man', 'India', 'Iraq', 'Iran', 'Iceland', 'Italy', 'Jersey', 'Jamaica', 'Jordan', 'Japan', 'Kenya', 'Kyrgyzstan', 'Cambodia', 'Kiribati', 'Saint Kitts and Nevis', 'North Korea', 'South Korea', 'Kuwait', 'Kazakhstan', 'Laos', 'Lebanon', 'St. Lucia', 'Liechtenstein', 'Sri Lanka', 'Liberia', 'Lesotho', 'Lithuania', 'Luxembourg', 'Latvia', 'Libya', 'Morocco', 'Monaco', 'Moldova', 'Montenegro', 'Madagascar', 'Republic of Macedonia', 'Mali', 'Myanmar [Burma]', 'Mongolia', 'Mauritania', 'Malta', 'Mauritius', 'Maldives', 'Malawi', 'Mexico', 'Malaysia', 'Mozambique', 'Namibia', 'Niger', 'Nigeria', 'Nicaragua', 'Netherlands', 'Norway', 'Nepal', 'Nauru', 'New Zealand', 'Oman', 'Panama', 'Peru', 'Philippines', 'Pakistan', 'Poland', 'Puerto Rico', 'Palestine', 'Portugal', 'Paraguay', 'Qatar', 'Romania', 'Serbia', 'Russia', 'Rwanda', 'Saudi Arabia', 'Seychelles', 'Sudan', 'Sweden', 'Singapore', 'Slovenia', 'Slovakia', 'Sierra Leone', 'Senegal', 'Somalia', 'Suriname', 'South Sudan', 'El Salvador', 'Syria', 'Swaziland', 'Chad', 'Togo', 'Thailand', 'Tajikistan', 'Timor-Leste', 'Turkmenistan', 'Tunisia', 'Tonga', 'Turkey', 'Trinidad and Tobago', 'Taiwan', 'Tanzania', 'Ukraine', 'Uganda', 'Unknown', 'United States', 'Uruguay', 'Uzbekistan', 'Venezuela', 'U.S. Virgin Islands', 'Vietnam', 'Vanuatu', 'Samoa', 'Kosovo', 'Yemen', 'South Africa', 'Zambia', 'Zimbabwe']
CONTINENT_NAMES = ['Asia', 'Europe', 'North America', 'South America', 'Australia', 'Antarctica', 'Africa']
def get_variable_type(v):
    '''
    Detect whether a string represents list of variables or a single variable.
    Then return the most likely type of the variable.
    '''
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
