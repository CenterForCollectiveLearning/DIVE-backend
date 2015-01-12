'''
Functions for returning the data corresponding to a given visualization type and specification
'''
import os
from flask import Flask  # Don't do this
from utility import *
from db import MongoInstance as MI
from bson.objectid import ObjectId
from config import config

from data import get_delimiter

import numpy as np
import pandas as pd


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
def getVisualizationData(type, spec, conditional, pID):
    if requiredParams(type, spec):
        if type == 'treemap':
            return getTreemapData(spec, conditional, pID)
        elif type == 'piechart':
            return getPiechartData(spec, conditional, pID)
        elif type == 'geomap':
            return getGeoData(spec, conditional, pID)
        elif type == 'barchart':
            return getScatterplotData(spec, conditional, pID)
        elif type == 'linechart':
            return getScatterplotData(spec, conditional, pID)
        elif type == 'scatterplot':
            return getScatterplotData(spec, conditional, pID)
    else:
        return "Did not pass required parameters", 400


def getTreemapData(spec, conditional, pID):
    # Parse specification
    condition = spec['condition']['title']
    groupby = spec['groupBy']['title']
    dID = spec['aggregate']['dID']
    aggFn = 'sum'  # TODO: get this from an argument

    # Load dataset (GENERALIZE THIS)
    filename = MI.getData({'_id': ObjectId(dID)}, pID)[0]['filename']
    path = os.path.join(config['UPLOAD_FOLDER'], pID, filename)
    delim = get_delimiter(path)
    df = pd.read_table(path, sep=delim)

    if conditional.get(dID):
        # Convert from {title: val} to {title: [val]}
        # formattedConditional = dict([(k, [v]) for k, v in conditional[dID].items() if (v != 'All')])
        for k, v in conditional[dID].iteritems():
            if v != 'All':
                df = df[df[k] == v]
        cond_df = df
    else:
        cond_df = df
    group_obj = cond_df.groupby(groupby)
    finalSeries = group_obj.size()

    result = []
    for row in finalSeries.iteritems():
        result.append({
            groupby: row[0],
            'count': np.asscalar(np.int16(row[1]))
        })
    return result

def getGeoData(spec, conditional, pID):
    return getTreemapData(spec, conditional, pID)

def getPiechartData(spec, conditional, pID):
    return getTreemapData(spec, conditional, pID)

def getBarchartData(spec, conditional, pID):
    return getScatterplotData(spec, conditional, pID)

def getScatterplotData(spec, conditional, pID):
    agg = spec['aggregation']
    x = spec['x']['title']    
    dID = spec['object']['dID']

    filename = MI.getData({'_id': ObjectId(dID)}, pID)[0]['filename']
    path = os.path.join(app.config['UPLOAD_FOLDER'], pID, filename)
    delim = get_delimiter(path)
    df = pd.read_table(path, sep=delim)

    if conditional[dID]:
        # Convert from {title: val} to {title: [val]}
        # formattedConditional = dict([(k, [v]) for k, v in conditional[dID].items() if (v != 'All')])
        for k, v in conditional[dID].iteritems():
            print k, v
            if v != 'All':
                df = df[df[k] == v]
        cond_df = df
        print cond_df
    else:
        cond_df = df

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

    return result


def getLinechartData(spec, conditional, pID):
    return getScatterplotData(spec, conditional, pID)


def getConditionalData(spec, dID, pID):
    # Load dataset (GENERALIZE THIS)
    filename = MI.getData({'_id': ObjectId(dID)}, pID)[0]['filename']
    path = os.path.join(app.config['UPLOAD_FOLDER'], pID, filename)
    delim = get_delimiter(path)
    df = pd.read_table(path, sep=delim)

    unique_elements = sorted([e for e in pd.Series(df[spec['name']]).dropna().unique()])

    return unique_elements