from data import is_numeric
from utility import *
from itertools import combinations
from db import MongoInstance as MI
import numpy as np
from visualization_data import getVisualizationData


# Calculate some statistical properties of the data that goes into a visualization
def getVisualizationStats(pID, spec, viz_type):
    stats = {}
    viz_data = getVisualizationData(viz_type, spec, {}, pID)
    num_elements = len(viz_data)
    counts = [e['count'] for e in viz_data]
    std = np.std(counts)
    stats['num_elements'] = num_elements
    if np.isnan(std):
        stats['std'] = None
    else:
        stats['std'] = std

    if viz_type in ["scatterplot", "linechart"]:
        print "Numeric"


    return stats


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
        "treemap": [],
        "piechart": [],
        "geomap": [],
        "scatterplot": [],
        "linechart": []
    }

    RECOMPUTE = False

    if existing_specs and not RECOMPUTE:
        for spec in existing_specs:
            viz_type = spec['viz_type']
            specs_by_viz_type[viz_type].append(spec)
    else:
        specs_by_viz_type = {
            "treemap": getTreemapSpecs(pID, d, p, o),
            "piechart": getPiechartSpecs(pID, d, p, o),
            "geomap": getGeomapSpecs(pID, d, p, o),
            "scatterplot": getScatterplotSpecs(pID, d, p, o),
            "linechart": getLinechartSpecs(pID, d, p, o),
        }

        for viz_type, specs in specs_by_viz_type.iteritems():
            if specs:
                for spec in specs:
                    spec['viz_type'] = viz_type
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
                spec['stats'] = getVisualizationStats(pID, spec, 'treemap')

                # Don't aggregate on uniformly distributed columns
                if spec['stats']['num_elements'] > 1:
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
                spec['stats'] = getVisualizationStats(pID, spec, 'geomap')
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
                    'x': {'index': index, 'title': headers[index]},
                    'object': {'dID': dID, 'title': dataset_titles[dID]},
                    'aggregation': True,
                    'condition': {'index': None, 'title': None},
                    'chosen': None,
                }
                spec['stats'] = getVisualizationStats(pID, spec, 'scatterplot')
                specs.append(spec)                
    return specs