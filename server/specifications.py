from utility import *
from itertools import combinations

#####################################################################
# 1. GROUP every entity by a non-unique attribute (for factors, group by factors but score by number of distinct. For continuous, discretize the range) 
#   1b. If attribute represents another object, also add aggregation by that object's attributes
# 2. AGGREGATE by some function (could be count)
# 3. QUERY by another non-unique attribute
#####################################################################

# TODO Incorporate ontologies
def getTreemapSpecs(datasets, properties, ontologies):
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
                specs.append({
                    'viz_type': 'treemap',
                    'aggregate': {'dID': dID, 'title': dataset_titles[dID]},
                    'groupBy': {'index': index, 'title': headers[index]},
                    'condition': {'index': None, 'title': None},
                    'chosen': False
                })
    return specs

def getPiechartSpecs(datasets, properties, ontologies):
    return getTreemapSpecs(datasets, properties, ontologies)

# TODO Reduce redunancy with treemap specs
def getGeomapSpecs(datasets, properties, ontologies):
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


def getBarchartSpecs(datasets, properties, ontologies):
    return getScatterplotSpecs(datasets, properties, ontologies)

def getLinechartSpecs(datasets, properties, ontologies):
    return getScatterplotSpecs(datasets, properties, ontologies)

def getScatterplotSpecs(datasets, properties, ontologies):
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

    print "SCATTERPLOT SPECS", specs
    return specs