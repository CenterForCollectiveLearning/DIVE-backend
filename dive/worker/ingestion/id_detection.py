from dive.base.constants import DataType
from dive.worker.ingestion.type_classes import IntegerType, StringType

import logging
logger = logging.getLogger(__name__)

ID_IS_STRINGS = ['id', 'Id', 'ID']
ID_POST_STRINGS = ['id', 'Id', 'ID']
ID_IN_STRINGS = ['subject', 'Subject', 'name', 'Name']


def detect_id_from_name(field_name):
    name_is_id = False
    for id_is_string in ID_IS_STRINGS:
        if field_name == id_is_string:
            name_is_id = True
    for id_in_string in ID_IN_STRINGS:
        if id_in_string in field_name:
            name_is_id = True
    for id_post_string in ID_POST_STRINGS:
        if field_name.endswith(id_post_string):
            name_is_id = True
    return name_is_id


def detect_id(field_name, field_type, is_unique):
    is_id = False

    # Unique strings with ID, name, or subject
    if (field_type in [ DataType.STRING.value ]) and (is_unique):
        return detect_id_from_name(field_name)

    # Integers with ID in name
    if (field_type in [ DataType.INTEGER.value ]):
        return detect_id_from_name(field_name)

    return is_id
