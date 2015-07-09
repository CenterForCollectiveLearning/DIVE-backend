from data.access import is_numeric
from itertools import combinations
from data.db import MongoInstance as MI
from viz_stats import *

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

    specs_by_category = {
        "shares": [],
        "time series": [],
        "distribution": [],
        # "comparison": []
    }

    spec_functions = {
        "shares": getSharesSpecs,
        "time series": getTimeSeriesSpecs,
        # "comparison": getComparisonSpecs,
        "distribution": getDistributionsSpecs
    }

    RECOMPUTE = True

    # if existing_specs and not RECOMPUTE:
    #     for spec in existing_specs:
    #         category = spec['category']
    #         specs_by_category[category].append(spec)
    # else:
    if RECOMPUTE:
        specs_by_category = {}
        for category, spec_function in spec_functions.iteritems():
            specs = spec_function(pID, d, p, o)
            for spec in specs:
                spec['category'] = category

                if spec['group']:
                    spec['groupBy'] = spec['group']['by']['title']
                else:
                    spec['groupBy'] = None

            # Persistence
            # if specs:
            #     sIDs = MI.postSpecs(pID, specs)
            #     for i, spec in enumerate(specs):
            #         spec['sID'] = sIDs[i]
            #         del spec['_id']

            specs_by_category[category] = specs

        return specs_by_category

def getSharesSpecs(pID, datasets, properties, ontologies):
    start_time = time()
    print "Getting shares specs"

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
                    'group': {
                      'by': { 'index': index, 'title': headers[index] }, 
                      'on': None,
                      'function': 'count',
                    },
                    'category': 'shares',
                    'chosen': None,
                }
                stat_time = time()
                # spec['stats'] = {}
                spec['stats'] = getVisualizationStats('time series', spec, {}, config, pID)

                # Don't aggregate on uniformly distributed columns
                # if spec['stats']['count'] > 1:
                specs.append(spec)
    print "Got shares specs, time:", time() - start_time
    return specs


def getComparisonSpecs(pID, datasets, properties, ontologies):
    specs = []
    dataset_titles = dict([(d['dID'], d['title']) for d in datasets])

    for p in properties:
        dID = p['dID']

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
                    'compare': {'index': index, 'title': headers[index]},
                    'object': {'dID': dID, 'title': dataset_titles[dID]},
                    'category': 'comparison',
                }

                spec['stats'] = getVisualizationStats('comparison', spec, {}, config, pID)

                # Don't aggregate on uniformly distributed columns
                # if spec['stats']['count'] > 1:
                specs.append(spec)
    return specs   


def getDistributionsSpecs(pID, datasets, properties, ontologies):
    start_time = time()
    print "Getting distribution specs"

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
                    'group': {
                      'by': { 'index': index, 'title': headers[index] }, 
                      'on': None,
                      'function': 'count',
                    },
                    'condition': {'index': None, 'title': None},
                    'category': 'distribution',
                    'chosen': None,
                }
                stat_time = time()
                # spec['stats'] = {}
                spec['stats'] = getVisualizationStats('time series', spec, {}, config, pID)

                # Don't aggregate on uniformly distributed columns
                # if spec['stats']['count'] > 1:
                specs.append(spec)
    print "Got shares specs, time:", time() - start_time
    return specs


def getTimeSeriesSpecs(pID, datasets, properties, ontologies):
    start_time = time()
    print "Getting time series specs"

    specs = []
    dataset_titles = dict([(d['dID'], d['title']) for d in datasets])

    # No grouping


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
        specs.append({
            'aggregate': {'dID': dID, 'title': dataset_titles[dID]},
            'group': None,
            'category': 'time series'
        })
        for index in non_uniques:
            type = types[index]

            # Aggregate on each factor attribute
            # TODO: Group numeric attributes with smart binning
            if not is_numeric(type):
                spec = {
                    'aggregate': {'dID': dID, 'title': dataset_titles[dID]},
                    'group': {
                      'by': { 'index': index, 'title': headers[index] }, 
                      'on': None,
                      'function': 'count',
                    },
                    'category': 'time series',
                    'chosen': None,
                }
                stat_time = time()
                spec['stats'] = getVisualizationStats('time series', spec, {}, config, pID)

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
                    'group': {
                      'by': { 'index': index, 'title': headers[index] }, 
                      'on': None,
                      'function': 'count',
                    },
                    'chosen': None,
                }
                spec['stats'] = getVisualizationStats('treemap', spec, {}, config, pID)

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
                    'group': {
                      'by': { 'index': index, 'title': headers[index] }, 
                      'on': None,
                      'function': 'count',
                    },
                    'condition': {'index': None, 'title': None},
                    'chosen': None,
                }
                spec['stats'] = getVisualizationStats('geomap', spec, {}, config, pID)
                specs.append(spec)
    return specs


def getBarchartSpecs(pID, datasets, properties, ontologies):
    return getScatterplotSpecs(pID, datasets, properties, ontologies)

def getLinechartSpecs(pID, datasets, properties, ontologies):
    return getScatterplotSpecs(pID, datasets, properties, ontologies)

# def getScatterplotSpecs(pID, datasets, properties, ontologies):
#     specs = []
#     dataset_titles = dict([(d['dID'], d['title']) for d in datasets])

#     # Single-dataset numeric vs. numeric
#     for p in properties:
#         dID = p['dID']
#         # TODO Perform this as a database query with a specific document?
#         relevant_ontologies = [ o for o in ontologies if ((o['source_dID'] == dID) or (o['target_dID'] == dID))]

#         types = p['types']
#         uniques = p['uniques']
#         headers = p['headers']
#         numeric_indices = [i for (i, type) in enumerate(types) if is_numeric(type)]

#         # # Two numeric rows
#         # for (index_a, index_b) in combinations(numeric_indices, 2):
#         #     specs.append({
#         #         'category': 'scatterplot',
#         #         'x': {'index': index_a, 'title': headers[index_a]},
#         #         'y': {'index': index_b, 'title': headers[index_b]},
#         #         'aggregation': False,
#         #         'object': {'dID': dID, 'title': dataset_titles[dID]},
#         #         'chosen': False
#         #     })

#         # Aggregating by a numeric row
#         for index in numeric_indices:
#             type = types[index]
#             title = headers[index]

#             if is_numeric(type):
#                 spec = {
#                     'x': {'index': index, 'title': headers[index], 'type' : type},
#                     'object': {'dID': dID, 'title': dataset_titles[dID]},
#                     'aggregation': True,
#                     'condition': {'index': None, 'title': None},
#                     'chosen': None,
#                 }
#                 spec['stats'] = getVisualizationStats('scatterplot', spec, {}, config, pID)
#                 specs.append(spec)                
#     return specs
