elementwise_functions = {
    'add': '+',
    'subtract': '-',
    'multiply': '*',
    'divide': '/'
}

# Value indicates whether function has been implemented
binning_procedures = {
    'freedman': True,
    'sturges': False,
    'scott': False,
    'shimazaki': False,
    'bayesian': False
}

from single_field_single_type_specs import *
from single_field_multi_type_specs import *
from multi_field_single_type_specs import *
from mixed_field_multi_type_specs import *
from multi_field_multi_type_specs import *
