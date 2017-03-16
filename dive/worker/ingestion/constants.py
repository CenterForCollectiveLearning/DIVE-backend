from enum import Enum

class GeneralDataType(Enum):
    CATEGORICAL = 'c'
    TEMPORAL = 't'
    QUANTIATIVE = 'q'
    C = CATEGORICAL
    T = TEMPORAL
    Q = QUANTIATIVE


class GeneralDataCharacteristic(Enum):
    NOMINAL = 'nom'
    ORDINAL = 'ord'
    CONTINUOUS = 'cat'


class DataType(Enum):
    # Fundamental
    INTEGER = 'integer'
    STRING = 'string'
    DECIMAL = 'decimal'
    BOOLEAN = 'boolean'

    # Special strings
    TEXT = 'text'
    URL = 'url'

    # Geographic
    LATITUDE = 'latitude'
    LONGITUDE = 'longitude'
    CITY = 'city'
    COUNTRY_CODE_2 = 'countryCode2'
    COUNTRY_CODE_3 = 'countryCode3'
    COUNTRY_NAME = 'countryName'
    CONTINENT_NAME = 'continentName'

    # Temporal
    DATETIME = 'datetime'
    DATE = 'date'
    TIME = 'time'
    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'

numeric_types = [
    DataType.INTEGER.value,
    DataType.DECIMAL.value
]

quantitative_types = [
    DataType.INTEGER.value,
    DataType.DECIMAL.value,
]

temporal_types = [
    DataType.DATETIME.value,
    DataType.DATE.value,
    DataType.TIME.value,
    DataType.YEAR.value,
    DataType.MONTH.value,
    DataType.DAY.value
]

categorical_types = [
    DataType.STRING.value,
    DataType.BOOLEAN.value,
    DataType.TEXT.value,
    DataType.URL.value,
    DataType.CITY.value,
    DataType.COUNTRY_CODE_2.value,
    DataType.COUNTRY_CODE_3.value,
    DataType.COUNTRY_NAME.value,
    DataType.CONTINENT_NAME.value,
]


specific_to_general_type = {}
for data_type in quantitative_types:
    specific_to_general_type[data_type] = GeneralDataType.Q.value
for data_type in temporal_types:
    specific_to_general_type[data_type] = GeneralDataType.T.value
for data_type in categorical_types:
    specific_to_general_type[data_type] = GeneralDataType.C.value


class DataTypeWeights(Enum):
    # Fundamental
    INTEGER = 8
    STRING = 2
    DECIMAL = 6
    BOOLEAN = 7

    # Special strings
    TEXT = 8
    URL = 8

    # Geographic
    LATITUDE = 10
    LONGITUDE = 10
    CITY = 10
    COUNTRY_CODE_2 = 10
    COUNTRY_CODE_3 = 10
    COUNTRY_NAME = 10
    CONTINENT_NAME = 10

    # Temporal
    DATETIME = 1
    DATE = 1
    TIME = 1
    YEAR = 1
    MONTH = 1
    DAY = 1


class PseudoType(Enum):
    LIST = 'list'

class FileType(Enum):
    CSV = 'csv'
    TSV = 'tsv'
    XLS = 'xls'
    XLSX = 'xlsx'
