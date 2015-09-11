'''
Get and compute whole-dataset properties
'''

def get_dataset_properties(dID, pID, path=None):
    ''' Get whole-dataset properties (recompute if doesnt exist) '''
    stored_properties = MI.getDatasetProperty({'dID': dID}, pID)
    if stored_properties:
        return stored_properties[0]
    else:
        return compute_dataset_properties(dID, pID, path=path)


def compute_dataset_properties(dID, pID, path=None):
    ''' Compute and return dictionary containing whole-dataset properties '''
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
