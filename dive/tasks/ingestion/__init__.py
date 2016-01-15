from enum import Enum

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


specific_to_general_type = {
    DataType.STRING.value: 'c',
    DataType.BOOLEAN.value: 'c',
    DataType.TEXT.value: 'c',
    DataType.URL.value: 'c',
    DataType.CITY.value: 'c',
    DataType.COUNTRY_CODE_2.value: 'c',
    DataType.COUNTRY_CODE_3.value: 'c',
    DataType.COUNTRY_NAME.value: 'c',
    DataType.CONTINENT_NAME.value: 'c',
    DataType.INTEGER.value: 'q',
    DataType.DECIMAL.value: 'q',
    DataType.DATETIME.value: 't',
    DataType.DATE.value: 't',
}

class DataTypeWeights(Enum):
    # Fundamental
    INTEGER = 6
    STRING = 1
    DECIMAL = 4
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
    DATETIME = 5
    DATE = 5
    TIME = 5
    YEAR = 5
    MONTH = 5
    DAY = 5


class PseudoType(Enum):
    LIST = 'list'

class FileType(Enum):
    CSV = 'csv'
    TSV = 'tsv'
    XLS = 'xls'
    XLSX = 'xlsx'
