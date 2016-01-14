from enum import Enum

class ModelName(Enum):
    PROJECT = 'project'
    DATASET = 'dataset'
    DATASET_PROPERTIES = 'dataset_properties'
    FIELD_PROPERTIES = 'field_properties'
    SPEC = 'spec'
    EXPORTED_SPEC = 'exported_spec'
    REGRESSION = 'regression'
    EXPORTED_REGRESSION = 'exported_regression'
    DOCUMENT = 'document'
    RELATIONSHIP = 'relationship'
    GROUP = 'group'
    USER = 'user'
