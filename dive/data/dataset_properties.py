'''
Get and compute whole-dataset properties
'''
import pandas as pd

from dive.data.access import get_data, get_delimiter
from dive.data.type_detection import get_column_types, detect_time_series
from dive.db import db_access

from bson.objectid import ObjectId
from in_memory_data import InMemoryData as IMD


def get_dataset_properties(dataset_id, project_id, path=None):
    ''' Get whole-dataset properties (recompute if doesnt exist) '''

    RECOMPUTE = True
    stored_properties = db_access.get_dataset_properties(project_id, dataset_id)

    if stored_properties and not RECOMPUTE:
        return stored_properties[0]
    else:
        return compute_dataset_properties(dataset_id, project_id, path=path)


def compute_dataset_properties(dataset_id, project_id, path=None):
    ''' Compute and return dictionary containing whole
    import pandas as pd-dataset properties '''
    if not path:
        path = db_access.get_dataset(project_id, dataset_id)['path']
    df = get_data(path=path).fillna('')  # TODO turn fillna into an argument
    header = df.columns.values
    n_rows, n_cols = df.shape
    types = get_column_types(df)
    time_series = detect_time_series(df)
    if time_series:
        structure = 'wide'
    else:
        structure = 'long'

    extension = path.rsplit('.', 1)[1]

    column_attrs = [{'name': header[i], 'type': types[i], 'column_id': i} for i in range(0, n_cols)]

    properties = {
        'field_names': column_attrs,
        'header': list(header),
        'n_rows': n_rows,
        'n_cols': n_cols,
        'filetype': extension,
        'structure': structure,
        'time_series': time_series
    }

    db_access.insert_data_properties(project_id, dataset_id, **properties)
    del properties['_id']

    return properties
