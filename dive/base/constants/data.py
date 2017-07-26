from enum import Enum

class GeneralDataType(Enum):
    CATEGORICAL = 'c'
    TEMPORAL = 't'
    QUANTIATIVE = 'q'
    C = CATEGORICAL
    T = TEMPORAL
    Q = QUANTIATIVE


class Scale(Enum):
    NOMINAL = 'nominal'
    ORDINAL = 'ordinal'
    CONTINUOUS = 'continuous'


class DataType(Enum):
    STRING = 'string'
    BOOLEAN = 'boolean'
    TEXT = 'text'
    URL = 'url'

    INTEGER = 'integer'
    DECIMAL = 'decimal'

    # Geographic
    CITY = 'city'
    LATITUDE = 'latitude'
    LONGITUDE = 'longitude'
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

specific_type_to_general_type = {}
for data_type in quantitative_types:
    specific_type_to_general_type[data_type] = GeneralDataType.Q.value
for data_type in temporal_types:
    specific_type_to_general_type[data_type] = GeneralDataType.T.value
for data_type in categorical_types:
    specific_type_to_general_type[data_type] = GeneralDataType.C.value

continuous_types = [
    DataType.INTEGER.value,
    DataType.DECIMAL.value,
    DataType.DATETIME.value,
    DataType.DATE.value,
    DataType.TIME.value,
]

ordinal_types = [
    DataType.YEAR.value,
    DataType.MONTH.value,
    DataType.DAY.value
]

nominal_types = [
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

specific_type_to_scale = {}
for data_type in continuous_types:
    specific_type_to_scale[data_type] = Scale.CONTINUOUS.value
for data_type in ordinal_types:
    specific_type_to_scale[data_type] = Scale.ORDINAL.value
for data_type in nominal_types:
    specific_type_to_scale[data_type] = Scale.NOMINAL.value

class DataTypeWeights(Enum):
    # Fundamental
    INTEGER = 8
    STRING = 4
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
