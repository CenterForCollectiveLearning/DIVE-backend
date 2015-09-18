'''
Get and compute whole-dataset properties
'''
import pandas as pd

from dive.data.access import get_data, get_delimiter
from dive.data.type_detection import get_column_types, detect_time_series
from dive.db.db import MongoInstance as MI

from bson.objectid import ObjectId
from in_memory_data import InMemoryData as IMD


def get_dataset_properties(dID, pID, path=None):
    ''' Get whole-dataset properties (recompute if doesnt exist) '''

    RECOMPUTE = True
    stored_properties = MI.getDatasetProperty({'dID': dID}, pID)
    if stored_properties and not RECOMPUTE:
        return stored_properties[0]
    else:
        return compute_dataset_properties(dID, pID, path=path)


def compute_dataset_properties(dID, pID, path=None):
    ''' Compute and return dictionary containing whole
    import pandas as pd-dataset properties '''
    if not path:
        path = MI.getData({'_id': ObjectId(dID)}, pID)[0]['path']
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
        'dID': dID,
        'column_attrs': column_attrs,
        'header': list(header),
        'rows': n_rows,
        'cols': n_cols,
        'filetype': extension,
        'structure': structure,
        'time_series': time_series
    }

    MI.setDatasetProperty(properties, pID)
    del properties['_id']

    return properties
