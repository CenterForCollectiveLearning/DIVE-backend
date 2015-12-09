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
    DataType.DATETIME.value,
    DataType.DATE.value,
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
    DATETIME = 10
    DATE = 10
    TIME = 10
    YEAR = 10
    MONTH = 10
    DAY = 10


class PseudoType(Enum):
    LIST = 'list'

class FileType(Enum):
    CSV = 'csv'
    TSV = 'tsv'
    XLS = 'xls'
    XLSX = 'xlsx'
