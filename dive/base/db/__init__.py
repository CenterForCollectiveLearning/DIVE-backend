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
    AGGREGATION = 'summary'
    COMPARISON = 'comparison'
    CORRELATION = 'correlation'
    DATASET = 'dataset'
    DATASET_PROPERTIES = 'dataset_properties'
    DOCUMENT = 'document'
    EXPORTED_AGGREGATION = 'exported_summary'
    EXPORTED_COMPARISON = 'exported_comparison'
    EXPORTED_CORRELATION = 'exported_correlation'
    EXPORTED_REGRESSION = 'exported_regression'
    EXPORTED_SPEC = 'exported_spec'
    FEEDBACK = 'feedback'
    FIELD_PROPERTIES = 'field_properties'
    INTERACTION_TERM = 'interaction_term'
    PRELOADED_DATASET = 'preloaded_dataset'
    PROJECT = 'project'
    REGRESSION = 'regression'
    RELATIONSHIP = 'relationship'
    SPEC = 'spec'
    TEAM = 'team'
    USER = 'user'


def row_to_dict(r, custom_fields=[]):
    d = { c.name: getattr(r, c.name) for c in r.__table__.columns }
    if custom_fields:
        for custom_field in custom_fields:
            d[custom_field] = getattr(r, custom_field)
    return d
