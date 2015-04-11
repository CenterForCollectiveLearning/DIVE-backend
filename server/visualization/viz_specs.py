from data.access import is_numeric
from itertools import combinations
from data.db import MongoInstance as MI
from viz_stats import getVisualizationStats

from time import time

#####################################################################
# 1. GROUP every entity by a non-unique attribute (for factors, group by factors but score by number of distinct. For continuous, discretize the range) 
#   1b. If attribute represents another object, also add aggregation by that object's attributes
# 2. AGGREGATE by some function (could be count)
# 3. QUERY by another non-unique attribute
#####################################################################
def getVisualizationSpecs(pID):
    d = MI.getData(None, pID)
    p = MI.getProperty(None, pID)
    o = MI.getOntology(None, pID)

    existing_specs = MI.getSpecs(pID, {})

    specs_by_viz_type = {
        "shares": [],
        "time series": [],
        "distributions": []
    }

    spec_functions = {
        "shares": getSharesSpecs,
        "time series": getTimeSeriesSpecs,
        "distributions": getDistributionsSpecs
    }

    RECOMPUTE = True

    # if existing_specs and not RECOMPUTE:
    #     for spec in existing_specs:
    #         viz_type = spec['viz_type']
    #         specs_by_viz_type[viz_type].append(spec)
    # else:
    if RECOMPUTE:
        specs_by_viz_type = {}
        for viz_type, spec_function in spec_functions.iteritems():
            specs = spec_function(pID, d, p, o)
            for spec in specs:
                spec['viz_type'] = viz_type

            # Persistence
            if specs:
                sIDs = MI.postSpecs(pID, specs)
                for i, spec in enumerate(specs):
                    spec['sID'] = sIDs[i]
                    del spec['_id']

            specs_by_viz_type[viz_type] = specs

        return specs_by_viz_type

def getSharesSpecs(pID, datasets, properties, ontologies):
    specs = []
    dataset_titles = dict([(d['dID'], d['title']) for d in datasets])
    return specs

def getDistributionsSpecs(pID, datasets, properties, ontologies):
    specs = []
    dataset_titles = dict([(d['dID'], d['title']) for d in datasets])
    return specs

def getTimeSeriesSpecs(pID, datasets, properties, ontologies):
    start_time = time()
    print "Getting time series specs"

    specs = []
    dataset_titles = dict([(d['dID'], d['title']) for d in datasets])

    for p in properties:
        dID = p['dID']
        # TODO Perform this as a database query with a specific document?
        relevant_ontologies = [ o for o in ontologies if ((o['source_dID'] == dID) or (o['target_dID'] == dID))]

        types = p['types']
        uniques = p['uniques']
        headers = p['headers']
        non_uniques = [i for (i, unique) in enumerate(uniques) if not unique]

        # For all non-unique attributes
        # TODO filter out columns in which all have the same attribute
        for index in non_uniques:
            type = types[index]

            # Aggregate on each factor attribute
            # TODO: Group numeric attributes with smart binning
            if not is_numeric(type):
                spec = {
                    'aggregate': {'dID': dID, 'title': dataset_titles[dID]},
                    'groupBy': {'index': index, 'title': headers[index]},
                    'condition': {'index': None, 'title': None},
                    'chosen': None,
                }
                stat_time = time()
                spec['stats'] = getVisualizationStats('time series', spec, {}, pID)
                print "Stat time", time() - stat_time

                # Don't aggregate on uniformly distributed columns
                # if spec['stats']['count'] > 1:
                specs.append(spec)
    print "Got time series specs, time:", time() - start_time
    return specs   


# TODO Incorporate ontologies
def getTreemapSpecs(pID, datasets, properties, ontologies):
    specs = []
    dataset_titles = dict([(d['dID'], d['title']) for d in datasets])

    for p in properties:
        dID = p['dID']
        # TODO Perform this as a database query with a specific document?
        relevant_ontologies = [ o for o in ontologies if ((o['source_dID'] == dID) or (o['target_dID'] == dID))]

        types = p['types']
        uniques = p['uniques']
        headers = p['headers']
        non_uniques = [i for (i, unique) in enumerate(uniques) if not unique]

        # For all non-unique attributes
        # TODO filter out columns in which all have the same attribute
        for index in non_uniques:
            type = types[index]

            # Aggregate on each factor attribute
            # TODO: Group numeric attributes with smart binning
            if not is_numeric(type):
                spec = {
                    'aggregate': {'dID': dID, 'title': dataset_titles[dID]},
                    'groupBy': {'index': index, 'title': headers[index]},
                    'condition': {'index': None, 'title': None},
                    'chosen': None,
                }
                spec['stats'] = getVisualizationStats('treemap', spec, {}, pID)

                # Don't aggregate on uniformly distributed columns
                if spec['stats']['count'] > 1:
                    specs.append(spec)
    return specs

def getPiechartSpecs(pID, datasets, properties, ontologies):
    return getTreemapSpecs(pID, datasets, properties, ontologies)

# TODO Reduce redundancy with treemap specs
def getGeomapSpecs(pID, datasets, properties, ontologies):
    specs = []
    dataset_titles = dict([(d['dID'], d['title']) for d in datasets])

    for p in properties:
        dID = p['dID']
        # TODO Perform this as a database query with a specific document?
        relevant_ontologies = [ o for o in ontologies if ((o['source_dID'] == dID) or (o['target_dID'] == dID))]

        types = p['types']
        uniques = p['uniques']
        headers = p['headers']
        non_uniques = [i for (i, unique) in enumerate(uniques) if not unique]

        for index in non_uniques:
            type = types[index]
            if type in ['countryCode2', 'countryCode3', 'countryName']:
                spec = {
                    'aggregate': {'dID': dID, 'title': dataset_titles[dID]},
                    'groupBy': {'index': index, 'title': headers[index]},
                    'condition': {'index': None, 'title': None},
                    'chosen': None,
                }
                spec['stats'] = getVisualizationStats('geomap', spec, {}, pID)
                specs.append(spec)
    return specs


def getBarchartSpecs(pID, datasets, properties, ontologies):
    return getScatterplotSpecs(pID, datasets, properties, ontologies)

def getLinechartSpecs(pID, datasets, properties, ontologies):
    return getScatterplotSpecs(pID, datasets, properties, ontologies)

def getScatterplotSpecs(pID, datasets, properties, ontologies):
    specs = []
    dataset_titles = dict([(d['dID'], d['title']) for d in datasets])

    # Single-dataset numeric vs. numeric
    for p in properties:
        dID = p['dID']
        # TODO Perform this as a database query with a specific document?
        relevant_ontologies = [ o for o in ontologies if ((o['source_dID'] == dID) or (o['target_dID'] == dID))]

        types = p['types']
        uniques = p['uniques']
        headers = p['headers']
        numeric_indices = [i for (i, type) in enumerate(types) if is_numeric(type)]

        # # Two numeric rows
        # for (index_a, index_b) in combinations(numeric_indices, 2):
        #     specs.append({
        #         'viz_type': 'scatterplot',
        #         'x': {'index': index_a, 'title': headers[index_a]},
        #         'y': {'index': index_b, 'title': headers[index_b]},
        #         'aggregation': False,
        #         'object': {'dID': dID, 'title': dataset_titles[dID]},
        #         'chosen': False
        #     })

        # Aggregating by a numeric row
        for index in numeric_indices:
            type = types[index]
            title = headers[index]

            if is_numeric(type):
                spec = {
                    'x': {'index': index, 'title': headers[index], 'type' : type},
                    'object': {'dID': dID, 'title': dataset_titles[dID]},
                    'aggregation': True,
                    'condition': {'index': None, 'title': None},
                    'chosen': None,
                }
                spec['stats'] = getVisualizationStats('scatterplot', spec, {}, pID)
                specs.append(spec)                
    return specs