from enum import Enum


class DataType(Enum):
    INTEGER = 'integer'
    STRING = 'string'
    DECIMAL = 'decimal'
    BOOLEAN = 'boolean'
    DATETIME = 'datetime'
    MONTH_NAME = 'monthName'
    DAY_NAME = 'dayName'
    COUNTRY_CODE_2 = 'countryCode2'
    COUNTRY_CODE_3 = 'countryCode3'
    COUNTRY_NAME = 'countryName'
    CONTINENT_NAME = 'continentName'

class DataTypeWeights(Enum):
    INTEGER = 6
    STRING = 1
    DECIMAL = 4
    BOOLEAN = 7
    DATETIME = 3
    MONTH_NAME = 8
    DAY_NAME = 8
    COUNTRY_CODE_2 = 10
    COUNTRY_CODE_3 = 10
    COUNTRY_NAME = 10
    CONTINENT_NAME = 10

class PseudoType(Enum):
    LIST = 'list'

class FileType(Enum):
    CSV = 'csv'
    TSV = 'tsv'
    XLS = 'xls'
    XLSX = 'xlsx'
