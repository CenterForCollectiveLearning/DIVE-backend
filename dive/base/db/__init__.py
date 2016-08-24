from enum import Enum

class AuthStatus(Enum):
    SUCCESS = 'success'
    ERROR = 'error'

class AuthMessage(Enum):
    USERNAME_NOT_FOUND = 'Username not found'
    EMAIL_NOT_FOUND = 'E-mail not found'
    INCORRECT_CREDENTIALS = 'Incorrect credentials'

class AuthErrorType(Enum):
    EMAIL = 'email'
    USERNAME = 'username'
    GENERAL = 'general'

class ModelName(Enum):
    PROJECT = 'project'
    DATASET = 'dataset'
    DATASET_PROPERTIES = 'dataset_properties'
    FIELD_PROPERTIES = 'field_properties'
    SPEC = 'spec'
    EXPORTED_SPEC = 'exported_spec'
    REGRESSION = 'regression'
    EXPORTED_REGRESSION = 'exported_regression'
    INTERACTION_TERM = 'interaction_term'
    AGGREGATION = 'summary'
    EXPORTED_AGGREGATION = 'exported_summary'
    CORRELATION = 'correlation'
    EXPORTED_CORRELATION = 'exported_correlation'
    COMPARISON = 'comparison'
    EXPORTED_COMPARISON = 'exported_comparison'
    DOCUMENT = 'document'
    RELATIONSHIP = 'relationship'
    GROUP = 'group'
    USER = 'user'


def row_to_dict(r, custom_fields=[]):
    d = { c.name: getattr(r, c.name) for c in r.__table__.columns }
    if custom_fields:
        for custom_field in custom_fields:
            d[custom_field] = getattr(r, custom_field)
    return d
