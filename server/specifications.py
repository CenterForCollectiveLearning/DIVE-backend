from data import is_numeric
from utility import *
from itertools import combinations
from db import MongoInstance as MI
import numpy as np
from visualization_data import getVisualizationData

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

    specs_by_viz_type = {
        "treemap": getTreemapSpecs(pID, d, p, o),
        "piechart": getPiechartSpecs(pID, d, p, o),
        "geomap": getGeomapSpecs(pID, d, p, o),
        "scatterplot": getScatterplotSpecs(pID, d, p, o),
        "linechart": getLinechartSpecs(pID, d, p, o),
        # "barchart": getBarchartSpecs(d, p, o),
        # "network": getNetworkSpecs(d, p, o)
    }

    for viz_type, specs in specs_by_viz_type.iteritems():
        if specs:
            sIDs = MI.postSpecs(pID, specs) 
            for i, spec in enumerate(specs):
                spec['sID'] = sIDs[i]
                del spec['_id']
    return specs_by_viz_type

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

        for index in non_uniques:
            type = types[index]
            if not is_numeric(type):
                spec = {
                    'viz_type': 'treemap',
                    'aggregate': {'dID': dID, 'title': dataset_titles[dID]},
                    'groupBy': {'index': index, 'title': headers[index]},
                    'condition': {'index': None, 'title': None},
                    'chosen': False,
                    'stats': {}
                }

                # Get stats about visualization
                viz_data = getVisualizationData('treemap', spec, {}, pID)
                num_elements = len(viz_data)
                counts = [e['count'] for e in viz_data]
                std = np.std(counts)
                spec['stats']['num_elements'] = num_elements
                spec['stats']['std'] = std

                specs.append(spec)
    return specs

def getPiechartSpecs(pID, datasets, properties, ontologies):
    return getTreemapSpecs(pID, datasets, properties, ontologies)

# TODO Reduce redunancy with treemap specs
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
            if not is_numeric(type) and (type == 'country'):
                specs.append({
                    'viz_type': 'treemap',
                    'aggregate': {'dID': dID, 'title': dataset_titles[dID]},
                    'groupBy': {'index': index, 'title': headers[index]},
                    'condition': {'index': None, 'title': None},
                    'chosen': False
                })
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
                specs.append({
                    'viz_type': 'scatterplot',
                    'x': {'index': index, 'title': headers[index]},
                    'object': {'dID': dID, 'title': dataset_titles[dID]},
                    'aggregation': True,
                    'chosen': False
                })
    return specs