'''
Functions for returning the data corresponding to a given visualization type and specification
'''
import os
from flask import Flask  # Don't do this
from utility import *
from db import MongoInstance as MI
from bson.objectid import ObjectId
from config import config

from data import get_delimiter, read_file

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

    dataset = MI.getData({'_id': ObjectId(dID)}, pID)[0]
    path = dataset['path']
    header, df = read_file(path)

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
    country_code_3_to_id = {"AFG": 4, "ALA": 248, "ALB": 8, "DZA": 12, "ASM": 16, "AND": 20, "AGO": 24, "AIA": 660, "ATA": 10, "ATG": 28, "ARG": 32, "ARM": 51, "ABW": 533, "AUS": 36, "AUT": 40, "AZE": 31, "BHS": 44, "BHR": 48, "BGD": 50, "BRB": 52, "BLR": 112, "BEL": 56, "BLZ": 84, "BEN": 204, "BMU": 60, "BTN": 64, "BOL": 68, "BES": 535, "BIH": 70, "BWA": 72, "BVT": 74, "BRA": 76, "IOT": 86, "BRN": 96, "BGR": 100, "BFA": 854, "BDI": 108, "KHM": 116, "CMR": 120, "CAN": 124, "CPV": 132, "CYM": 136, "CAF": 140, "TCD": 148, "CHL": 152, "CHN": 156, "CXR": 162, "CCK": 166, "COL": 170, "COM": 174, "COG": 178, "COD": 180, "COK": 184, "CRI": 188, "CIV": 384, "HRV": 191, "CUB": 192, "CUW": 531, "CYP": 196, "CZE": 203, "DNK": 208, "DJI": 262, "DMA": 212, "DOM": 214, "ECU": 218, "EGY": 818, "SLV": 222, "GNQ": 226, "ERI": 232, "EST": 233, "ETH": 231, "FLK": 238, "FRO": 234, "FJI": 242, "FIN": 246, "FRA": 250, "GUF": 254, "PYF": 258, "ATF": 260, "GAB": 266, "GMB": 270, "GEO": 268, "DEU": 276, "GHA": 288, "GIB": 292, "GRC": 300, "GRL": 304, "GRD": 308, "GLP": 312, "GUM": 316, "GTM": 320, "GGY": 831, "GIN": 324, "GNB": 624, "GUY": 328, "HTI": 332, "HMD": 334, "VAT": 336, "HND": 340, "HKG": 344, "HUN": 348, "ISL": 352, "IND": 356, "IDN": 360, "IRN": 364, "IRQ": 368, "IRL": 372, "IMN": 833, "ISR": 376, "ITA": 380, "JAM": 388, "JPN": 392, "JEY": 832, "JOR": 400, "KAZ": 398, "KEN": 404, "KIR": 296, "PRK": 408, "KOR": 410, "KWT": 414, "KGZ": 417, "LAO": 418, "LVA": 428, "LBN": 422, "LSO": 426, "LBR": 430, "LBY": 434, "LIE": 438, "LTU": 440, "LUX": 442, "MAC": 446, "MKD": 807, "MDG": 450, "MWI": 454, "MYS": 458, "MDV": 462, "MLI": 466, "MLT": 470, "MHL": 584, "MTQ": 474, "MRT": 478, "MUS": 480, "MYT": 175, "MEX": 484, "FSM": 583, "MDA": 498, "MCO": 492, "MNG": 496, "MNE": 499, "MSR": 500, "MAR": 504, "MOZ": 508, "MMR": 104, "NAM": 516, "NRU": 520, "NPL": 524, "NLD": 528, "NCL": 540, "NZL": 554, "NIC": 558, "NER": 562, "NGA": 566, "NIU": 570, "NFK": 574, "MNP": 580, "NOR": 578, "OMN": 512, "PAK": 586, "PLW": 585, "PSE": 275, "PAN": 591, "PNG": 598, "PRY": 600, "PER": 604, "PHL": 608, "PCN": 612, "POL": 616, "PRT": 620, "PRI": 630, "QAT": 634, "REU": 638, "ROU": 642, "RUS": 643, "RWA": 646, "BLM": 652, "SHN": 654, "KNA": 659, "LCA": 662, "MAF": 663, "SPM": 666, "VCT": 670, "WSM": 882, "SMR": 674, "STP": 678, "SAU": 682, "SEN": 686, "SRB": 688, "SYC": 690, "SLE": 694, "SGP": 702, "SXM": 534, "SVK": 703, "SVN": 705, "SLB": 90, "SOM": 706, "ZAF": 710, "SGS": 239, "SSD": 728, "ESP": 724, "LKA": 144, "SDN": 729, "SUR": 740, "SJM": 744, "SWZ": 748, "SWE": 752, "CHE": 756, "SYR": 760, "TWN": 158, "TJK": 762, "TZA": 834, "THA": 764, "TLS": 626, "TGO": 768, "TKL": 772, "TON": 776, "TTO": 780, "TUN": 788, "TUR": 792, "TKM": 795, "TCA": 796, "TUV": 798, "UGA": 800, "UKR": 804, "ARE": 784, "GBR": 826, "USA": 840, "UMI": 581, "URY": 858, "UZB": 860, "VUT": 548, "VEN": 862, "VNM": 704, "VGB": 92, "VIR": 850, "WLF": 876, "ESH": 732, "YEM": 887, "ZMB": 894, "ZWE": 716}
    result = getTreemapData(spec, conditional, pID)
    for r in result:
        r['id'] = country_code_3_to_id.get(r['countryCode3'], '')
    return result 

def getPiechartData(spec, conditional, pID):
    return getTreemapData(spec, conditional, pID)

def getBarchartData(spec, conditional, pID):
    return getScatterplotData(spec, conditional, pID)

def getScatterplotData(spec, conditional, pID):
    agg = spec['aggregation']
    x = spec['x']['title']    
    dID = spec['object']['dID']

    dataset = MI.getData({'_id': ObjectId(dID)}, pID)[0]
    path = dataset['path']
    header, df = read_file(path)

    if conditional.get(dID):
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
        result = [{x: x_val, y: y_val} for (x_val, y_val) in zip(df[x], df[y])]

    return result


def getLinechartData(spec, conditional, pID):
    return getScatterplotData(spec, conditional, pID)


def getConditionalData(spec, dID, pID):
    # Load dataset (GENERALIZE THIS)
    dataset = MI.getData({'_id': ObjectId(dID)}, pID)[0]
    filename = dataset['filename']
    path = dataset['path']
    header, df = read_file(path)
    df = pd.DataFrame(data)

    unique_elements = sorted([e for e in pd.Series(df[spec['name']]).dropna().unique()])

    return unique_elements