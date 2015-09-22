from enum import Enum


class DataType(Enum):
    INTEGER = 'integer'
    STRING = 'string'
    FLOAT = 'float'
    COMPLEX = 'complex'
    BOOLEAN = 'boolean'
    DATETIME = 'datetime'
    MONTH_NAME = 'monthName'
    DAY_NAME = 'dayName'
    COUNTRY_CODE_2 = 'countryCode2'
    COUNTRY_CODE_3 = 'countryCode3'
    COUNTRY_NAME = 'countryName'
    CONTINENT_NAME = 'continentName'

class PseudoType(Enum):
    LIST = 'list'

class FileType(Enum):
    CSV = 'csv'
    TSV = 'tsv'
    XLS = 'xls'
    XLSX = 'xlsx'
