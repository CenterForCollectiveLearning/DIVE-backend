'''
From https://github.com/okfn/messytables/blob/master/messytables/types.py
'''
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
    name = None

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


class SpecificCellType(CellType):
    examples = []

    def test(self, value):
        if isinstance(value, self.result_type) and value in self.examples:
            return True
        return False


class IntegerType(CellType):
    ''' Integer field '''
    name = DataType.INTEGER.value
    weight = DataTypeWeights.INTEGER.value
    result_type = int
    regex = re.compile("^-?[0-9]+$")

    def cast(self, value):
        logger.info(value)
        if regex.match(value):
            return int(value)

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


class StringType(CellType):
    ''' A string or other unconverted type. '''
    name = DataType.STRING.value
    weight = DataTypeWeights.STRING.value
    result_type = unicode

    def cast(self, value):
        if value is None:
            return None
        if isinstance(value, self.result_type):
            return value
        try:
            return unicode(value)
        except UnicodeEncodeError:
            return str(value)


class DecimalType(CellType):
    ''' Decimal number, ``decimal.Decimal`` or float numbers. '''
    name = DataType.DECIMAL.value
    weight = DataTypeWeights.DECIMAL.value
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


class BooleanType(CellType):
    ''' A boolean field. Matches true/false, yes/no and 0/1 by default,
    but a custom set of values can be optionally provided.
    '''
    name = DataType.BOOLEAN.value
    weight = DataTypeWeights.BOOLEAN.value
    result_type = bool

    true_values = ('yes', 'true')
    false_values = ('no', 'false')

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


class DateType(CellType):
    '''
    The date type is special in that it also includes a specific
    date format that is used to parse the date, additionally to the
    basic type information.
    '''
    formats = DATE_FORMATS
    name = DataType.DATETIME.value
    result_type = datetime.datetime
    weight = DataTypeWeights.DATETIME.value

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
    name = DataType.DATETIME.value
    weight = DataTypeWeights.DATETIME.value
    result_type = datetime.datetime

    def cast(self, value):
        if value in ('', None):
            return None
        return dparser.parse(value)


class MonthType(SpecificCellType):
    name = DataType.MONTH.value
    weight = DataTypeWeights.MONTH.value
    result_type = unicode
    examples = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']


class DayType(SpecificCellType):
    name = DataType.DAY.value
    weight = DataTypeWeights.DAY.value
    result_type = unicode
    examples = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


class CountryCode2Type(SpecificCellType):
    name = DataType.COUNTRY_CODE_2.value
    weight = DataTypeWeights.COUNTRY_CODE_2.value
    result_type = unicode
    examples = ['AD', 'AE', 'AF', 'AG', 'AL', 'AM', 'AO', 'AR', 'AT', 'AU', 'AW', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BM', 'BN', 'BO', 'BR', 'BT', 'BW', 'BY', 'CA', 'CD', 'CF', 'CG', 'CH', 'CI', 'CL', 'CM', 'CN', 'CO', 'CR', 'CU', 'CV', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DO', 'DZ', 'EC', 'EE', 'EG', 'ER', 'ES', 'ET', 'FI', 'FM', 'FO', 'FR', 'GA', 'GB', 'GE', 'GH', 'GI', 'GL', 'GM', 'GN', 'GQ', 'GR', 'GT', 'GW', 'GY', 'HK', 'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IQ', 'IR', 'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KN', 'KP', 'KR', 'KW', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS', 'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MG', 'MK', 'ML', 'MM', 'MN', 'MR', 'MT', 'MU', 'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NE', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR', 'NZ', 'OM', 'PA', 'PE', 'PH', 'PK', 'PL', 'PR', 'PS', 'PT', 'PY', 'QA', 'RO', 'RS', 'RU', 'RW', 'SA', 'SC', 'SD', 'SE', 'SG', 'SI', 'SK', 'SL', 'SN', 'SO', 'SR', 'SS', 'ST', 'SV', 'SY', 'SZ', 'TD', 'TG', 'TH', 'TJ', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TW', 'TZ', 'UA', 'UG', 'UNK', 'US', 'UY', 'UZ', 'VE', 'VI', 'VN', 'VU', 'WS', 'XK', 'YE', 'ZA', 'ZM', 'ZW']


class CountryCode3Type(SpecificCellType):
    name = DataType.COUNTRY_CODE_3.value
    weight = DataTypeWeights.COUNTRY_CODE_3.value
    result_type = unicode
    examples = ['AND', 'ARE', 'AFG', 'ATG', 'ALB', 'ARM', 'AGO', 'ARG', 'AUT', 'AUS', 'ABW', 'AZE', 'BIH', 'BRB', 'BGD', 'BEL', 'BFA', 'BGR', 'BHR', 'BDI', 'BEN', 'BMU', 'BRN', 'BOL', 'BRA', 'BTN', 'BWA', 'BLR', 'CAN', 'COD', 'CAF', 'COG', 'CHE', 'CIV', 'CHL', 'CMR', 'CHN', 'COL', 'CRI', 'CUB', 'CPV', 'CYP', 'CZE', 'DEU', 'DJI', 'DNK', 'DOM', 'DZA', 'ECU', 'EST', 'EGY', 'ERI', 'ESP', 'ETH', 'FIN', 'FSM', 'FRO', 'FRA', 'GAB', 'GBR', 'GEO', 'GHA', 'GIB', 'GRL', 'GMB', 'GIN', 'GNQ', 'GRC', 'GTM', 'GNB', 'GUY', 'HKG', 'HND', 'HRV', 'HTI', 'HUN', 'IDN', 'IRL', 'ISR', 'IMN', 'IND', 'IRQ', 'IRN', 'ISL', 'ITA', 'JEY', 'JAM', 'JOR', 'JPN', 'KEN', 'KGZ', 'KHM', 'KIR', 'KNA', 'PRK', 'KOR', 'KWT', 'KAZ', 'LAO', 'LBN', 'LCA', 'LIE', 'LKA', 'LBR', 'LSO', 'LTU', 'LUX', 'LVA', 'LBY', 'MAR', 'MCO', 'MDA', 'MNE', 'MDG', 'MKD', 'MLI', 'MMR', 'MNG', 'MRT', 'MLT', 'MUS', 'MDV', 'MWI', 'MEX', 'MYS', 'MOZ', 'NAM', 'NER', 'NGA', 'NIC', 'NLD', 'NOR', 'NPL', 'NRU', 'NZL', 'OMN', 'PAN', 'PER', 'PHL', 'PAK', 'POL', 'PRI', 'PSE', 'PRT', 'PRY', 'QAT', 'ROU', 'SRB', 'RUS', 'RWA', 'SAU', 'SYC', 'SDN', 'SWE', 'SGP', 'SVN', 'SVK', 'SLE', 'SEN', 'SOM', 'SUR', 'SSD', 'STP', 'SLV', 'SYR', 'SWZ', 'TCD', 'TGO', 'THA', 'TJK', 'TLS', 'TKM', 'TUN', 'TON', 'TUR', 'TTO', 'TWN', 'TZA', 'UKR', 'UGA', 'UNK', 'USA', 'URY', 'UZB', 'VEN', 'VIR', 'VNM', 'VUT', 'WSM', 'SCG', 'YEM', 'ZAF', 'ZMB', 'ZWE']

class CountryNameType(SpecificCellType):
    name = DataType.COUNTRY_NAME.value
    weight = DataTypeWeights.COUNTRY_NAME.value
    result_type = unicode
    examples = ['Andorra', 'United Arab Emirates', 'Afghanistan', 'Antigua and Barbuda', 'Albania', 'Armenia', 'Angola', 'Argentina', 'Austria', 'Australia', 'Aruba', 'Azerbaijan', 'Bosnia and Herzegovina', 'Barbados', 'Bangladesh', 'Belgium', 'Burkina Faso', 'Bulgaria', 'Bahrain', 'Burundi', 'Benin', 'Bermuda', 'Brunei', 'Bolivia', 'Brazil', 'Bhutan', 'Botswana', 'Belarus', 'Canada', 'Democratic Republic of Congo', 'Central African Republic', 'Congo [Republic]', 'Switzerland', 'Chile', 'Cameroon', 'China', 'Colombia', 'Costa Rica', 'Cuba', 'Cape Verde', 'Cyprus', 'Czech Republic', 'Germany', 'Djibouti', 'Denmark', 'Dominican Republic', 'Algeria', 'Ecuador', 'Estonia', 'Egypt', 'Eritrea', 'Spain', 'Ethiopia', 'Finland', 'Micronesia', 'Faroe Islands', 'France', 'Gabon', 'United Kingdom', 'Georgia', 'Ghana', 'Gibraltar', 'Greenland', 'The Gambia', 'Guinea', 'Equatorial Guinea', 'Greece', 'Guatemala', 'Guinea-Bissau', 'Guyana', 'Hong Kong', 'Honduras', 'Croatia', 'Haiti', 'Hungary', 'Indonesia', 'Ireland', 'Israel', 'Isle of Man', 'India', 'Iraq', 'Iran', 'Iceland', 'Italy', 'Jersey', 'Jamaica', 'Jordan', 'Japan', 'Kenya', 'Kyrgyzstan', 'Cambodia', 'Kiribati', 'Saint Kitts and Nevis', 'North Korea', 'South Korea', 'Kuwait', 'Kazakhstan', 'Laos', 'Lebanon', 'St. Lucia', 'Liechtenstein', 'Sri Lanka', 'Liberia', 'Lesotho', 'Lithuania', 'Luxembourg', 'Latvia', 'Libya', 'Morocco', 'Monaco', 'Moldova', 'Montenegro', 'Madagascar', 'Republic of Macedonia', 'Mali', 'Myanmar [Burma]', 'Mongolia', 'Mauritania', 'Malta', 'Mauritius', 'Maldives', 'Malawi', 'Mexico', 'Malaysia', 'Mozambique', 'Namibia', 'Niger', 'Nigeria', 'Nicaragua', 'Netherlands', 'Norway', 'Nepal', 'Nauru', 'New Zealand', 'Oman', 'Panama', 'Peru', 'Philippines', 'Pakistan', 'Poland', 'Puerto Rico', 'Palestine', 'Portugal', 'Paraguay', 'Qatar', 'Romania', 'Serbia', 'Russia', 'Rwanda', 'Saudi Arabia', 'Seychelles', 'Sudan', 'Sweden', 'Singapore', 'Slovenia', 'Slovakia', 'Sierra Leone', 'Senegal', 'Somalia', 'Suriname', 'South Sudan', 'El Salvador', 'Syria', 'Swaziland', 'Chad', 'Togo', 'Thailand', 'Tajikistan', 'Timor-Leste', 'Turkmenistan', 'Tunisia', 'Tonga', 'Turkey', 'Trinidad and Tobago', 'Taiwan', 'Tanzania', 'Ukraine', 'Uganda', 'Unknown', 'United States', 'Uruguay', 'Uzbekistan', 'Venezuela', 'U.S. Virgin Islands', 'Vietnam', 'Vanuatu', 'Samoa', 'Kosovo', 'Yemen', 'South Africa', 'Zambia', 'Zimbabwe']

class ContinentNameType(SpecificCellType):
    name = DataType.CONTINENT_NAME.value
    weight = DataTypeWeights.CONTINENT_NAME.value
    result_type = unicode
    examples = ['Asia', 'Europe', 'North America', 'South America', 'Australia', 'Antarctica', 'Africa']
