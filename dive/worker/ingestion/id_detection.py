from dive.worker.ingestion import DataType
from dive.worker.ingestion.type_classes import IntegerType, StringType

import logging
logger = logging.getLogger(__name__)

def detect_id(field_name, field_type, is_unique):
    ID_IS_STRINGS = ['id', 'Id', 'ID']
    ID_POST_STRINGS = ['id', 'Id', 'ID']
    ID_IN_STRINGS = ['subject', 'Subject', 'name', 'Name']

    is_id = False

    # Unique strings with ID, name, or subject
    if (field_type in [ DataType.STRING.value ]) and (is_unique):
        for id_in_string in ID_IN_STRINGS:
            if id_in_string in field_name:
                is_id = True

    # Integers with ID in name
    if (field_type in [ DataType.INTEGER.value ]):
        for id_is_string in ID_IS_STRINGS:
            if field_name is id_is_string:
                is_id = True
        for id_post_string in ID_POST_STRINGS:
            if field_name.endswith(id_post_string):
                is_id = True

    return is_id
