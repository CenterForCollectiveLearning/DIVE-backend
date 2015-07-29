from data.db import MongoInstance as MI

# Retrieve proeprties given dataset_docs
# TODO Accept list of dIDs
def get_properties(pID, datasets) :
    properties = []
    _property_labels = []

    _find_doc = {"$or" : map(lambda x: {'dID' : x['dID']}, datasets)}
    _all_properties = MI.getProperty(_find_doc, pID)

    if len(_all_properties):
        _properties_by_dID = {}
        for p in _all_properties:
            dID = p['dID']
            del p['dID']
            del p['tID']
            _properties_by_dID[dID] = p

    # If not in DB, compute
    else:
        _properties_by_dID = compute_properties(pID, datasets)

    for _dID, _properties_data in _properties_by_dID.iteritems():
        for _label, _type, _unique, _unique_values in zip(_properties_data['headers'], _properties_data['types'], _properties_data['uniques'], _properties_data['unique_values']):
            if _label in _property_labels:
                properties[_property_labels.index(_label)]['dIDs'].append[_dID]
            else:
                _property_labels.append(_label)
                properties.append({
                    'label': _label,
                    'type': _type,
                    'unique': _unique,
                    'values': _unique_values,
                    'dIDs': [_dID]
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

# Compute properties of all passed datasets
# Currently only getting properties by column
# Arguments: pID + dataset documents
# Returns a mapping from dIDs to properties
def compute_properties(pID, dataset_docs):
    properties_by_dID = {}

    for dataset in dataset_docs:
        property_dict = {
            'types': [],
            'label': [],
            'values': [],
            'unique': [],
            'normality': {},
            'stats': {},
            'misc': {}
        }
        dID = dataset['dID']
        df = get_data(pID=pID, dID=dID)
        df = df.fillna('')

        labels = df.columns.values
        property_dict['label'] = labels.tolist()

        print "Calculating properties for dID", dID
        # Statistical properties
        # Only conduct on certain types?
        print "\tDescribing datasets"
        df_stats = df.describe()
        df_stats_dict = json.loads(df_stats.to_json())
        df_stats_list = []
        for l in labels:
            if l in df_stats_dict:
                df_stats_list.append(df_stats_dict[l])
            else:
                df_stats_list.append({})
        property_dict['stats'] = df_stats_list

        ### Getting column types
        print "\tGetting types"
        types = get_column_types(df)
        property_dict['types'] = types
    
        ### Determining normality
        print "\tDetermining normality"
        start_time = time()
        normality = []
        for i, col in enumerate(df):
            type = types[i]
            if type in ["int", "float"]:
                try:
                    ## Coerce data vector to float
                    d = df[col].astype(np.float)
                    normality_result = stats.normaltest(d)
                except ValueError:
                    normality_result = None                    
            else:
                normality_result = None
            normality.append(normality_result)

        property_dict['normality'] = normality
        print "\t\t", time() - start_time, "seconds"
    
        ### Detecting if a column is unique
        print "\tDetecting uniques"
        start_time = time()
        # List of booleans -- is a column composed of unique elements?
        unique = [ detect_unique_list(df[col]) for col in df ]
        property_dict['unique'] = unique
        print "\t\t", time() - start_time, "seconds"

        ### Unique values for columns
        print "\tGetting unique values"
        start_time = time()
        unique_values = []
        raw_uniqued_values = [ get_unique(df[col]) for col in df ]
        for i, col in enumerate(raw_uniqued_values):
            type = types[i]
            if type in ["integer", "float"]:
                unique_values.append([])
            else:
                unique_values.append(col)
        property_dict['values'] = unique_values
        print "\t\t", time() - start_time, "seconds"

        # Save properties into collection
        tID = MI.upsertProperty(dID, pID, property_dict)

        properties_by_dID[dID] = property_dict
    return properties_by_dID
