import os
from flask import Flask  # Don't do this
from utility import *
from db import MongoInstance as MI
from bson.objectid import ObjectId

import numpy as np
import pandas as pd

app = Flask(__name__, static_path='/static')
UPLOAD_FOLDER = os.path.join(os.curdir, 'uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


vizToRequiredParams = {
    'treemap': ['aggregate', 'groupBy'],
    'piechart': ['aggregate', 'groupBy'],
    'geomap': ['aggregate', 'groupBy'],
    'barchart': ['aggregate', 'groupBy'],
    'scatterplot': ['x', 'object']
}


# Utility function to make sure all fields needed to create visualization type are passed
def requiredParams(type, spec):
    # for requiredParam in vizToRequiredParams[type]:
    #     if requiredParam not in spec:
    #         return False
    return True


# Check parameters and route to correct vizdata function
def getVisualizationData(type, spec, pID):
    if requiredParams(type, spec):
        if type == 'treemap':
            return getTreemapData(spec, pID)
        elif type == 'piechart':
            return getPiechartData(spec, pID)
        elif type == 'geo':
            return getGeoData(spec, pID)
        elif type == 'barchart':
            return getScatterplotData(spec, pID)
        elif type == 'linechart':
            return getScatterplotData(spec, pID)
        elif type == 'scatterplot':
            return getScatterplotData(spec, pID)
    else:
        return "Did not pass required parameters", 400


def getTreemapData(spec, pID):
    # Parse specification
    condition = spec['condition']['title']
    groupby = spec['groupBy']['title']
    dID = spec['aggregate']['dID']
    aggFn = 'sum'  # TODO: get this from an argument

    # Load dataset (GENERALIZE THIS)
    filename = MI.getData({'_id': ObjectId(dID)}, pID)[0]['filename']
    path = os.path.join(app.config['UPLOAD_FOLDER'], pID, filename)
    delim = get_delimiter(path)
    df = pd.read_table(path, sep=delim)

    cond_df = df
    group_obj = cond_df.groupby(groupby)
    finalSeries = group_obj.size()

    result = []
    for row in finalSeries.iteritems():
        result.append({
            groupby: row[0],
            'count': np.asscalar(np.int16(row[1]))
        })
    return {'result': result}

    # Compute
    #         if query[0] == '*':
    #             cond_df = df
    #         else:
    #             # Uses column indexing for now
    #             cond_df = df[df[condition].isin(query)]


def getPiechartData(spec, pID):
    return getTreemapData(spec, pID)


def getBarchartData(spec, pID):
    return getScatterplotData(spec, pID)

def getScatterplotData(spec, pID):
    agg = spec['aggregation']
    x = spec['x']['title']    
    dID = spec['object']['dID']

    filename = MI.getData({'_id': ObjectId(dID)}, pID)[0]['filename']
    path = os.path.join(app.config['UPLOAD_FOLDER'], pID, filename)
    delim = get_delimiter(path)
    df = pd.read_table(path, sep=delim)

    result = []
    if agg:
        cond_df = df
        group_obj = cond_df.groupby(x)
        finalSeries = group_obj.size()

        result = []
        for row in finalSeries.iteritems():
            result.append({
                x: row[0],
                'count': np.asscalar(np.int16(row[1]))
            })

    else:
        y = spec['y']['title']

        # Load dataset (GENERALIZE THIS)
        filename = MI.getData({'_id': ObjectId(dID)}, pID)[0]['filename']
        path = os.path.join(app.config['UPLOAD_FOLDER'], pID, filename)
        delim = get_delimiter(path)
        df = pd.read_table(path, sep=delim)

        result = [{x: x_val, y: y_val} for (x_val, y_val) in zip(df[x], df[y])]

    return {'result': result}


def getLinechartData(spec, pID):
    return getScatterplotData(spec, pID)


def getConditionalData(spec, pID):
    # Parse specification
    condition = spec['condition']['title']
    dID = spec['aggregate']['dID']

    # Load dataset (GENERALIZE THIS)
    filename = MI.getData({'_id': ObjectId(dID)}, pID)[0]['filename']
    path = os.path.join(app.config['UPLOAD_FOLDER'], pID, filename)
    delim = get_delimiter(path)
    df = pd.read_table(path, sep=delim)

    unique_elements = [{condition: e} for e in pd.Series(df[condition]).unique()]

    # return {'result': unique_elements}
    return {'result': []}