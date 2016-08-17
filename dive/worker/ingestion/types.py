import re
import locale
import decimal
import datetime
import dateutil.parser as dparser
from dateparser import DATE_FORMATS, is_date

from dive.worker.ingestion import DataType, DataTypeWeights

string_types = (str, unicode)

class CellType(object):
    ''' A cell type maintains information about the format
    of the cell, providing methods to check if a type is
    applicable to a given value and to convert a value to the
    type. '''

    guessing_weight = 1
    # the type that the result will have
    result_type = None

    def test(self, value):
        ''' Test if the value is of the given type. The
        default implementation calls ``cast`` and checks if
        that throws an exception. True or False'''
        if isinstance(value, self.result_type):
            return True
        try:
            self.cast(value)
            return True
        except:
            return False

    @classmethod
    def instances(cls):
        return [cls()]

    def cast(self, value):
        ''' Convert the value to the type. This may throw
        a quasi-random exception if conversion fails. '''
        return value

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def __hash__(self):
        return hash(self.__class__)

    def __repr__(self):
        return self.__class__.__name__.rsplit('Type', 1)[0]


class IntegerType(CellType):
    ''' Integer field '''
    weight = 6
    regex = "^-?[0-9]+$"

    def cast(self, value):
        if value in ('', None):
            return None
        try:
            value = float(value)
        except:
            return locale.atoi(value)

        if value.is_integer():
            return int(value)
        else:
            raise ValueError('Invalid integer: %s' % value)


class BoolType(CellType):
    ''' A boolean field. Matches true/false, yes/no and 0/1 by default,
    but a custom set of values can be optionally provided.
    '''
    guessing_weight = 7
    result_type = bool
    true_values = ('yes', 'true', '0')
    false_values = ('no', 'false', '1')

    def __init__(self, true_values=None, false_values=None):
        if true_values is not None:
            self.true_values = true_values
        if false_values is not None:
            self.false_values = false_values

    def cast(self, value):
        s = value.strip().lower()
        if value in ('', None):
            return None
        if s in self.true_values:
            return True
        if s in self.false_values:
            return False
        raise ValueError


class DecimalType(CellType):
    ''' Decimal number, ``decimal.Decimal`` or float numbers. '''
    guessing_weight = 4
    result_type = decimal.Decimal

    def cast(self, value):
        if value in ('', None):
            return None
        try:
            return decimal.Decimal(value)
        except:
            value = locale.atof(value)
            if sys.version_info < (2, 7):
                value = str(value)
            return decimal.Decimal(value)


class DateType(CellType):
    ''' The date type is special in that it also includes a specific
    date format that is used to parse the date, additionally to the
    basic type information. '''
    guessing_weight = 3
    formats = DATE_FORMATS
    result_type = datetime.datetime

    def __init__(self, format):
        self.format = format

    @classmethod
    def instances(cls):
        return [cls(v) for v in cls.formats]

    def test(self, value):
        if isinstance(value, string_types) and not is_date(value):
            return False
        return CellType.test(self, value)

    def cast(self, value):
        if isinstance(value, self.result_type):
            return value
        if value in ('', None):
            return None
        if self.format is None:
            return value
        return datetime.datetime.strptime(value, self.format)

    def __eq__(self, other):
        return (isinstance(other, DateType) and
                self.format == other.format)

    def __repr__(self):
        return "Date(%s)" % self.format

    def __hash__(self):
        return hash(self.__class__) + hash(self.format)


class DateUtilType(CellType):
    ''' The date util type uses the dateutil library to
    parse the dates. The advantage of this type over
    DateType is the speed and better date detection. However,
    it does not offer format detection.
    Do not use this together with the DateType'''
    guessing_weight = 3
    result_type = datetime.datetime

    def cast(self, value):
        if value in ('', None):
            return None
        return parser.parse(value)
