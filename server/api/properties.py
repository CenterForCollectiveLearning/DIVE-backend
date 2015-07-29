from data.db import MongoInstance as MI

# Retrieve proeprties given dataset_docs
# TODO Accept list of dIDs
def get_properties(pID, datasets) :
    properties = []
    _property_labels = []

    _find_doc = {"$or" : map(lambda x: {'dID' : x['dID']}, datasets)}
    _data_by_dataset = MI.getProperty(_find_doc, pID)

    for _dataset in _data_by_dataset:
        for _label, _type, _unique, _unique_values in zip(_dataset['headers'], _dataset['types'], _dataset['uniques'], _dataset['unique_values']):
            if _label in _property_labels:
                properties[_property_labels.index(_label)]['dIDs'].append[_dataset['dID']]
            else:
                _property_labels.append(_label)
                properties.append({
                    'label': _label,
                    'type': _type,
                    'unique': _unique,
                    'values': _unique_values,
                    'dIDs': [_dataset['dID']]
                })

    return properties

# Retrieve entities given datasets
def get_entities(pID, datasets):
    entities = []
    _properties = get_properties(pID, datasets)
    entities = filter(lambda x: x['type'] not in ['float', 'integer'], _properties)

    return entities

# Retrieve entities given datasets
def get_attributes(pID, datasets):
    attributes = []
    _properties = get_properties(pID, datasets)
    attributes = filter(lambda x: x['type'] in ['float', 'integer'], _properties)

    return attributes
