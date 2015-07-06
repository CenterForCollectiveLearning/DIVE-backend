# -*- coding: utf-8 -*-

'''
Functions for returning the data corresponding to a given visualization type and specification
'''
from flask import Flask  # Don't do this
from bson.objectid import ObjectId

from data.access import get_delimiter, get_data, detect_time_series
from data.db import MongoInstance as MI
from data.in_memory_data import InMemoryData as IMD

from itertools import combinations

import viz_stats

from config import config

import numpy as np
import pandas as pd
import scipy as sp
import math

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


# TODO just use regular strings?
aggregation_fn = {
    'sum': np.sum,
    'min': np.min,
    'max': np.max,
    'mean': np.mean,
    'count': np.size
}


# df = pd.DataFrame({'AAA': [4,5,6,7], 'BBB': [10,20,30,40], 'CCC': [100,50,-30,-50]})
# For now, dealing with a single aggregation
# formula = {'aggregate': {'field': 'AAA', 'operation': 'sum'}, 'condition': {'and': [{'field': 'AAA', 'operation': '>', 'criteria': 5}], 'or': [{'field': 'BBB', 'operation': '==', 'criteria': 10}]}, 'query': 'BBB'}
def getVisualizationDataFromFormula(formula, df):
    aggregate_arg = formula['aggregate']
    condition_arg = formula['condition']
    query_arg = formula['query']

    if not (aggregate_arg and condition_arg and query_arg):
        return "Did not pass required parameters", 400

    ### 1) Apply all conditionals
    # Conduct query strings for 'or' and 'and'
    query_strings = {
        'and': '',
        'or': ''
    }
    if condition_arg.get('and'):
        query_strings['and'] = ' & '.join(['%s %s %s' % (c['field'], c['operation'], c['criteria']) for c in condition_arg['and']])
    if condition_arg.get('or'):
        query_strings['or'] = ' | '.join(['%s %s %s' % (c['field'], c['operation'], c['criteria']) for c in condition_arg['or']])

    # Concatenate
    final_query_string = ''
    if query_strings['and'] and query_strings['or']:
        final_query_string = '%s | %s' % (query_strings['and'], query_strings['or'])
    elif query_strings['and'] and not query_strings['or']:
        final_query_string = query_strings['and']
    elif query_strings['or'] and not query_strings['and']:
        final_query_string = query_strings['or']
    if final_query_string:
        conditioned_df = df.query(final_query_string)

    ### 2) Aggregation / grouping
    # TODO Chain with agg?
    # TODO Deal with multiple aggregations?
    aggregated_df = None
    field = aggregate_arg['field']
    gb = conditioned_df.groupby(field)

    # If no query field is specified, always group by counts for now
    if query_arg:
        agg_operation = aggregation_fn[aggregate_arg['operation']]
        aggregated_df = gb.aggregate(agg_operation)
    else:
        aggregated_df = gb.size()

    ### 3) Incorporate query and format result
    aggregated_dict = aggregated_df.to_dict()
    if query_arg:
        aggregated_dict = aggregated_dict[query_arg]

    print "aggregated_dict", aggregated_dict
    result = [ {aggregate_arg['field']: k, 'value': v} for k, v in aggregated_dict.iteritems() ]
    return result, 200


def getVisualizationDataFromFormula(formula, pID):
    print "Getting viz data from formula", formula, pID

# Check parameters and route to correct vizdata function
def getVisualizationData(type, spec, conditional, config, pID):
    print "Getting visualization data", type, spec, conditional, config
    if requiredParams(type, spec):
        viz_data_functions = {
            'time series': getTimeSeriesData,
            'shares': getSharesData,
            'distribution': getDistributionData,
            'comparison': getComparisonsData,
        }
        return viz_data_functions[type](spec, conditional, config, pID)
    else:
        return "Did not pass required parameters", 400

def getRawData(type, spec, conditional, config, pID) :
    if type in ['treemap', 'geomap', 'piechart', 'time series'] :
        dID = spec['aggregate']['dID']

    elif type in ['comparison', 'scatterplot', 'barchart', 'linechart'] :
        dID = spec['object']['dID']

    df = get_data(pID=pID, dID=dID)
    time_series_obj = detect_time_series(df)
    if time_series_obj:
        time_series_names = time_series_obj['time_series']['names']

        # Removing date fields
        dropped_columns = []
        if conditional.get(dID) :
            for k, v in conditional[dID].iteritems():
                # For date conditionals, remove all date fields out the range            
                if (k == 'Start Date'):
                    dropped_columns.extend(time_series_names[:time_series_names.index(v)])
                if (k == 'End Date'):
                    dropped_columns.extend(time_series_names[time_series_names.index(v):])
    
        df = df.drop(dropped_columns, 1)

    # Subsetting rows based on conditional
    if conditional.get(dID) :
        for k, v in conditional[dID].iteritems():
            if (k in ['Start Date', 'End Date']):
                continue
            else:
                if (v != 'All') :
                    df = df[df[k] == v]
    cond_df = df
    return cond_df


def getComparisonsData(spec, conditional, config, pID):
    print "Getting comparisons data"
    compare_attr = spec['compare']['title']
    cond_df = getRawData('comparison', spec, conditional, config, pID).fillna(0)

    groupby = 'Brand'
    unique_elements = sorted([e for e in pd.Series(cond_df[compare_attr]).dropna().unique()])

    final_result = {}
    for (a, b) in combinations(unique_elements, 2):
        df_subset_a = cond_df[cond_df[compare_attr] == a]
        aggregated_a = df_subset_a.groupby(groupby).sum().transpose().sum().to_dict()

        df_subset_b = cond_df[cond_df[compare_attr] == b]
        aggregated_b = df_subset_b.groupby(groupby).sum().transpose().sum().to_dict()

        pair_result = []
        a_vals = []
        b_vals = []
        for k, val_a in aggregated_a.iteritems():
            if k in aggregated_b:
                val_b = aggregated_b[k]
                a_vals.append(math.log(val_a))
                b_vals.append(math.log(val_b))
                pair_result.append({a: val_a, b: val_b, 'label': k})
        final_result['%s\t%s' % (a, b)] = pair_result

    return final_result


def getSharesData(spec, conditional, config, pID):
    print "Getting shares data with spec:", spec
    group_obj = spec.get('group')
    group_by = group_obj['by']['title']
    group_function = group_obj['function']
    # group_on = group_obj['on']['title']
    
    # 1) Apply conditional
    cond_df = getRawData('treemap', spec, conditional, config, pID).fillna(0)

    # 2) Group on
    gb = cond_df.groupby(group_by)

    # 3) Aggregate
    # TODO Use .agg or .aggregate to pass in aggregation function as parameter
    if group_function is 'sum':
        aggregated = gb.sum()
    elif group_function is 'max':
        aggregated = gb.max()
    elif group_function is 'min':
        aggregated = gb.min()        
    elif group_function is 'count':
        aggregated = gb.size()
        # aggregated = gb.count()
    # Default to counting number of occurrences
    # Should do something more intelligent!
    else:  
        aggregated = gb.size()


    aggregated = aggregated.to_dict()
    print aggregated
    # aggregated = cond_df.groupby(group_by).sum().transpose().sum().to_dict()
    result = []
    for k, v in aggregated.iteritems():
        result.append({group_by: k, 'value': v})
    return result


def getDistributionData(spec, conditional, config, pID):
    print "Getting shares data"
    groupby = spec['group']['by']['title']
    
    cond_df = getRawData('treemap', spec, conditional, config, pID).fillna(0)
    
    aggregated = cond_df.groupby(groupby).sum().transpose().sum().to_dict()
    result = []
    for k, v in aggregated.iteritems():
        result.append({groupby: k, 'value': v})
    return result


def getTimeSeriesData(spec, conditional, config, pID):
    normalize = config.get('normalize', None)
    print "NORMALIZE FIELD:", normalize

    result = {}
    # Grouping by specific attribute
    print "Before getting raw data"
    cond_df = getRawData('treemap', spec, conditional, config, pID).fillna(0)

    print "Got raw data"
    if spec['group']['by']:
        groupby = spec['group']['by']['title']
        aggregated_series = cond_df.groupby(groupby).sum().transpose()
        aggregated_series_dict = aggregated_series.to_dict()

        if normalize:
            print "Normalizing data by date"
            total_vals_by_date = {}

            # Normalization
            for k, series in aggregated_series_dict.iteritems():
                formatted_series = []
                for date, val in series.iteritems():
                    if date not in total_vals_by_date:
                        total_vals_by_date[date] = val
                    else:
                        total_vals_by_date[date] += val

        for k, series in aggregated_series_dict.iteritems():
            formatted_series = []
            for date, val in series.iteritems():
                if normalize:
                    print "Normalizing"
                    val = val / total_vals_by_date[date]
                formatted_series.append({'date': date, 'value': val})
            result[k] = formatted_series

    # Top-level visualization
    else:
        aggregated_series = cond_df.sum(numeric_only=True).transpose()
        aggregated_series_dict = aggregated_series.to_dict()

        formatted_series = []
        for date, val in aggregated_series_dict.iteritems():
            formatted_series.append({'date': date, 'value': val})
        result['All'] = formatted_series

    return result

# def getTreemapData(spec, conditional, config, pID):
#     # Parse specification
#     # condition = spec['condition']['title']
#     group_obj = spec.get('group')
#     group_by = group_obj['by']['title']
#     group_function = group_obj['function']

#     cond_df = getRawData('treemap', spec, conditional, config, pID)

#     group_obj = cond_df.groupby(groupby)
#     print "GROUP_OBJ", group_obj
#     finalSeries = group_obj.size()

#     result = []
#     for row in finalSeries.iteritems():
#         result.append({
#             groupby: row[0],
#             'count': np.asscalar(np.int16(row[1]))
#         })
#     return result

country_name_to_id = {"Afghanistan": 4, "Åland Islands": 248, "Albania": 8, "Algeria": 12, "American Samoa": 16, "Andorra": 20, "Angola": 24, "Anguilla": 660, "Antarctica": 10, "Antigua and Barbuda": 28, "Argentina": 32, "Armenia": 51, "Aruba": 533, "Australia": 36, "Austria": 40, "Azerbaijan": 31, "Bahamas": 44, "Bahrain": 48, "Bangladesh": 50, "Barbados": 52, "Belarus": 112, "Belgium": 56, "Belize": 84, "Benin": 204, "Bermuda": 60, "Bhutan": 64, "Bolivia (Plurinational State of)": 68, "Bonaire, Sint Eustatius and Saba": 535, "Bosnia and Herzegovina": 70, "Botswana": 72, "Bouvet Island": 74, "Brazil": 76, "British Indian Ocean Territory": 86, "Brunei Darussalam": 96, "Bulgaria": 100, "Burkina Faso": 854, "Burundi": 108, "Cambodia": 116, "Cameroon": 120, "Canada": 124, "Cabo Verde": 132, "Cayman Islands": 136, "Central African Republic": 140, "Chad": 148, "Chile": 152, "China": 156, "Christmas Island": 162, "Cocos (Keeling) Islands": 166, "Colombia": 170, "Comoros": 174, "Congo": 178, "Congo (Democratic Republic of the)": 180, "Cook Islands": 184, "Costa Rica": 188, "Côte d'Ivoire": 384, "Croatia": 191, "Cuba": 192, "Curaçao": 531, "Cyprus": 196, "Czech Republic": 203, "Denmark": 208, "Djibouti": 262, "Dominica": 212, "Dominican Republic": 214, "Ecuador": 218, "Egypt": 818, "El Salvador": 222, "Equatorial Guinea": 226, "Eritrea": 232, "Estonia": 233, "Ethiopia": 231, "Falkland Islands (Malvinas)": 238, "Faroe Islands": 234, "Fiji": 242, "Finland": 246, "France": 250, "French Guiana": 254, "French Polynesia": 258, "French Southern Territories": 260, "Gabon": 266, "Gambia": 270, "Georgia": 268, "Germany": 276, "Ghana": 288, "Gibraltar": 292, "Greece": 300, "Greenland": 304, "Grenada": 308, "Guadeloupe": 312, "Guam": 316, "Guatemala": 320, "Guernsey": 831, "Guinea": 324, "Guinea-Bissau": 624, "Guyana": 328, "Haiti": 332, "Heard Island and McDonald Islands": 334, "Holy See": 336, "Honduras": 340, "Hong Kong": 344, "Hungary": 348, "Iceland": 352, "India": 356, "Indonesia": 360, "Iran (Islamic Republic of)": 364, "Iraq": 368, "Ireland": 372, "Isle of Man": 833, "Israel": 376, "Italy": 380, "Jamaica": 388, "Japan": 392, "Jersey": 832, "Jordan": 400, "Kazakhstan": 398, "Kenya": 404, "Kiribati": 296, "Korea (Democratic People's Republic of)": 408, "Korea (Republic of)": 410, "Kuwait": 414, "Kyrgyzstan": 417, "Lao People's Democratic Republic": 418, "Latvia": 428, "Lebanon": 422, "Lesotho": 426, "Liberia": 430, "Libya": 434, "Liechtenstein": 438, "Lithuania": 440, "Luxembourg": 442, "Macao": 446, "Macedonia (the former Yugoslav Republic of)": 807, "Madagascar": 450, "Malawi": 454, "Malaysia": 458, "Maldives": 462, "Mali": 466, "Malta": 470, "Marshall Islands": 584, "Martinique": 474, "Mauritania": 478, "Mauritius": 480, "Mayotte": 175, "Mexico": 484, "Micronesia (Federated States of)": 583, "Moldova (Republic of)": 498, "Monaco": 492, "Mongolia": 496, "Montenegro": 499, "Montserrat": 500, "Morocco": 504, "Mozambique": 508, "Myanmar": 104, "Namibia": 516, "Nauru": 520, "Nepal": 524, "Netherlands": 528, "New Caledonia": 540, "New Zealand": 554, "Nicaragua": 558, "Niger": 562, "Nigeria": 566, "Niue": 570, "Norfolk Island": 574, "Northern Mariana Islands": 580, "Norway": 578, "Oman": 512, "Pakistan": 586, "Palau": 585, "Palestine, State of": 275, "Panama": 591, "Papua New Guinea": 598, "Paraguay": 600, "Peru": 604, "Philippines": 608, "Pitcairn": 612, "Poland": 616, "Portugal": 620, "Puerto Rico": 630, "Qatar": 634, "Réunion": 638, "Romania": 642, "Russian Federation": 643, "Rwanda": 646, "Saint Barthélemy": 652, "Saint Helena, Ascension and Tristan da Cunha": 654, "Saint Kitts and Nevis": 659, "Saint Lucia": 662, "Saint Martin (French part)": 663, "Saint Pierre and Miquelon": 666, "Saint Vincent and the Grenadines": 670, "Samoa": 882, "San Marino": 674, "Sao Tome and Principe": 678, "Saudi Arabia": 682, "Senegal": 686, "Serbia": 688, "Seychelles": 690, "Sierra Leone": 694, "Singapore": 702, "Sint Maarten (Dutch part)": 534, "Slovakia": 703, "Slovenia": 705, "Solomon Islands": 90, "Somalia": 706, "South Africa": 710, "South Georgia and the South Sandwich Islands": 239, "South Sudan": 728, "Spain": 724, "Sri Lanka": 144, "Sudan": 729, "Suriname": 740, "Svalbard and Jan Mayen": 744, "Swaziland": 748, "Sweden": 752, "Switzerland": 756, "Syrian Arab Republic": 760, "Taiwan, Province of China": 158, "Tajikistan": 762, "Tanzania, United Republic of": 834, "Thailand": 764, "Timor-Leste": 626, "Togo": 768, "Tokelau": 772, "Tonga": 776, "Trinidad and Tobago": 780, "Tunisia": 788, "Turkey": 792, "Turkmenistan": 795, "Turks and Caicos Islands": 796, "Tuvalu": 798, "Uganda": 800, "Ukraine": 804, "United Arab Emirates": 784, "United Kingdom of Great Britain and Northern Ireland": 826, "United States of America": 840, "United States Minor Outlying Islands": 581, "Uruguay": 858, "Uzbekistan": 860, "Vanuatu": 548, "Venezuela (Bolivarian Republic of)": 862, "Viet Nam": 704, "Virgin Islands (British)": 92, "Virgin Islands (U.S.)": 850, "Wallis and Futuna": 876, "Western Sahara": 732, "Yemen": 887, "Zambia": 894, "Zimbabwe": 716}
country_code_2_to_id =  {"AF": 4, "AX": 248, "AL": 8, "DZ": 12, "AS": 16, "AD": 20, "AO": 24, "AI": 660, "AQ": 10, "AG": 28, "AR": 32, "AM": 51, "AW": 533, "AU": 36, "AT": 40, "AZ": 31, "BS": 44, "BH": 48, "BD": 50, "BB": 52, "BY": 112, "BE": 56, "BZ": 84, "BJ": 204, "BM": 60, "BT": 64, "BO": 68, "BQ": 535, "BA": 70, "BW": 72, "BV": 74, "BR": 76, "IO": 86, "BN": 96, "BG": 100, "BF": 854, "BI": 108, "KH": 116, "CM": 120, "CA": 124, "CV": 132, "KY": 136, "CF": 140, "TD": 148, "CL": 152, "CN": 156, "CX": 162, "CC": 166, "CO": 170, "KM": 174, "CG": 178, "CD": 180, "CK": 184, "CR": 188, "CI": 384, "HR": 191, "CU": 192, "CW": 531, "CY": 196, "CZ": 203, "DK": 208, "DJ": 262, "DM": 212, "DO": 214, "EC": 218, "EG": 818, "SV": 222, "GQ": 226, "ER": 232, "EE": 233, "ET": 231, "FK": 238, "FO": 234, "FJ": 242, "FI": 246, "FR": 250, "GF": 254, "PF": 258, "TF": 260, "GA": 266, "GM": 270, "GE": 268, "DE": 276, "GH": 288, "GI": 292, "GR": 300, "GL": 304, "GD": 308, "GP": 312, "GU": 316, "GT": 320, "GG": 831, "GN": 324, "GW": 624, "GY": 328, "HT": 332, "HM": 334, "VA": 336, "HN": 340, "HK": 344, "HU": 348, "IS": 352, "IN": 356, "ID": 360, "IR": 364, "IQ": 368, "IE": 372, "IM": 833, "IL": 376, "IT": 380, "JM": 388, "JP": 392, "JE": 832, "JO": 400, "KZ": 398, "KE": 404, "KI": 296, "KP": 408, "KR": 410, "KW": 414, "KG": 417, "LA": 418, "LV": 428, "LB": 422, "LS": 426, "LR": 430, "LY": 434, "LI": 438, "LT": 440, "LU": 442, "MO": 446, "MK": 807, "MG": 450, "MW": 454, "MY": 458, "MV": 462, "ML": 466, "MT": 470, "MH": 584, "MQ": 474, "MR": 478, "MU": 480, "YT": 175, "MX": 484, "FM": 583, "MD": 498, "MC": 492, "MN": 496, "ME": 499, "MS": 500, "MA": 504, "MZ": 508, "MM": 104, "NA": 516, "NR": 520, "NP": 524, "NL": 528, "NC": 540, "NZ": 554, "NI": 558, "NE": 562, "NG": 566, "NU": 570, "NF": 574, "MP": 580, "NO": 578, "OM": 512, "PK": 586, "PW": 585, "PS": 275, "PA": 591, "PG": 598, "PY": 600, "PE": 604, "PH": 608, "PN": 612, "PL": 616, "PT": 620, "PR": 630, "QA": 634, "RE": 638, "RO": 642, "RU": 643, "RW": 646, "BL": 652, "SH": 654, "KN": 659, "LC": 662, "MF": 663, "PM": 666, "VC": 670, "WS": 882, "SM": 674, "ST": 678, "SA": 682, "SN": 686, "RS": 688, "SC": 690, "SL": 694, "SG": 702, "SX": 534, "SK": 703, "SI": 705, "SB": 90, "SO": 706, "ZA": 710, "GS": 239, "SS": 728, "ES": 724, "LK": 144, "SD": 729, "SR": 740, "SJ": 744, "SZ": 748, "SE": 752, "CH": 756, "SY": 760, "TW": 158, "TJ": 762, "TZ": 834, "TH": 764, "TL": 626, "TG": 768, "TK": 772, "TO": 776, "TT": 780, "TN": 788, "TR": 792, "TM": 795, "TC": 796, "TV": 798, "UG": 800, "UA": 804, "AE": 784, "GB": 826, "US": 840, "UM": 581, "UY": 858, "UZ": 860, "VU": 548, "VE": 862, "VN": 704, "VG": 92, "VI": 850, "WF": 876, "EH": 732, "YE": 887, "ZM": 894, "ZW": 716}
country_code_3_to_id = {"AFG": 4, "ALA": 248, "ALB": 8, "DZA": 12, "ASM": 16, "AND": 20, "AGO": 24, "AIA": 660, "ATA": 10, "ATG": 28, "ARG": 32, "ARM": 51, "ABW": 533, "AUS": 36, "AUT": 40, "AZE": 31, "BHS": 44, "BHR": 48, "BGD": 50, "BRB": 52, "BLR": 112, "BEL": 56, "BLZ": 84, "BEN": 204, "BMU": 60, "BTN": 64, "BOL": 68, "BES": 535, "BIH": 70, "BWA": 72, "BVT": 74, "BRA": 76, "IOT": 86, "BRN": 96, "BGR": 100, "BFA": 854, "BDI": 108, "KHM": 116, "CMR": 120, "CAN": 124, "CPV": 132, "CYM": 136, "CAF": 140, "TCD": 148, "CHL": 152, "CHN": 156, "CXR": 162, "CCK": 166, "COL": 170, "COM": 174, "COG": 178, "COD": 180, "COK": 184, "CRI": 188, "CIV": 384, "HRV": 191, "CUB": 192, "CUW": 531, "CYP": 196, "CZE": 203, "DNK": 208, "DJI": 262, "DMA": 212, "DOM": 214, "ECU": 218, "EGY": 818, "SLV": 222, "GNQ": 226, "ERI": 232, "EST": 233, "ETH": 231, "FLK": 238, "FRO": 234, "FJI": 242, "FIN": 246, "FRA": 250, "GUF": 254, "PYF": 258, "ATF": 260, "GAB": 266, "GMB": 270, "GEO": 268, "DEU": 276, "GHA": 288, "GIB": 292, "GRC": 300, "GRL": 304, "GRD": 308, "GLP": 312, "GUM": 316, "GTM": 320, "GGY": 831, "GIN": 324, "GNB": 624, "GUY": 328, "HTI": 332, "HMD": 334, "VAT": 336, "HND": 340, "HKG": 344, "HUN": 348, "ISL": 352, "IND": 356, "IDN": 360, "IRN": 364, "IRQ": 368, "IRL": 372, "IMN": 833, "ISR": 376, "ITA": 380, "JAM": 388, "JPN": 392, "JEY": 832, "JOR": 400, "KAZ": 398, "KEN": 404, "KIR": 296, "PRK": 408, "KOR": 410, "KWT": 414, "KGZ": 417, "LAO": 418, "LVA": 428, "LBN": 422, "LSO": 426, "LBR": 430, "LBY": 434, "LIE": 438, "LTU": 440, "LUX": 442, "MAC": 446, "MKD": 807, "MDG": 450, "MWI": 454, "MYS": 458, "MDV": 462, "MLI": 466, "MLT": 470, "MHL": 584, "MTQ": 474, "MRT": 478, "MUS": 480, "MYT": 175, "MEX": 484, "FSM": 583, "MDA": 498, "MCO": 492, "MNG": 496, "MNE": 499, "MSR": 500, "MAR": 504, "MOZ": 508, "MMR": 104, "NAM": 516, "NRU": 520, "NPL": 524, "NLD": 528, "NCL": 540, "NZL": 554, "NIC": 558, "NER": 562, "NGA": 566, "NIU": 570, "NFK": 574, "MNP": 580, "NOR": 578, "OMN": 512, "PAK": 586, "PLW": 585, "PSE": 275, "PAN": 591, "PNG": 598, "PRY": 600, "PER": 604, "PHL": 608, "PCN": 612, "POL": 616, "PRT": 620, "PRI": 630, "QAT": 634, "REU": 638, "ROU": 642, "RUS": 643, "RWA": 646, "BLM": 652, "SHN": 654, "KNA": 659, "LCA": 662, "MAF": 663, "SPM": 666, "VCT": 670, "WSM": 882, "SMR": 674, "STP": 678, "SAU": 682, "SEN": 686, "SRB": 688, "SYC": 690, "SLE": 694, "SGP": 702, "SXM": 534, "SVK": 703, "SVN": 705, "SLB": 90, "SOM": 706, "ZAF": 710, "SGS": 239, "SSD": 728, "ESP": 724, "LKA": 144, "SDN": 729, "SUR": 740, "SJM": 744, "SWZ": 748, "SWE": 752, "CHE": 756, "SYR": 760, "TWN": 158, "TJK": 762, "TZA": 834, "THA": 764, "TLS": 626, "TGO": 768, "TKL": 772, "TON": 776, "TTO": 780, "TUN": 788, "TUR": 792, "TKM": 795, "TCA": 796, "TUV": 798, "UGA": 800, "UKR": 804, "ARE": 784, "GBR": 826, "USA": 840, "UMI": 581, "URY": 858, "UZB": 860, "VUT": 548, "VEN": 862, "VNM": 704, "VGB": 92, "VIR": 850, "WLF": 876, "ESH": 732, "YEM": 887, "ZMB": 894, "ZWE": 716}
def getGeoData(spec, conditional, config, pID):
    result = getTreemapData(spec, conditional, config, pID)

    group_by_variable = spec['group']['by']['title']
    group_by_index = spec['group']['by']['index']
    # Get type of field
    property = MI.getProperty({'dID': spec['aggregate']['dID']}, pID)[0]
    type = property['types'][group_by_index]

    for r in result:
        r['label'] = r[group_by_variable]
        if type == 'countryCode3':
            r['id'] = country_code_3_to_id.get(r[group_by_variable], '')
        elif type == 'countryCode2':
            r['id'] = country_code_2_to_id.get(r[group_by_variable], '')
        elif type == 'countryName':
            r['id'] = country_name_to_id.get(r[group_by_variable], '')
        else:
            return []
    return result 

def getScatterplotData(spec, conditional, config, pID):
    agg = spec['aggregation']
    x = spec['x']['title']    

    cond_df = getRawData('treemap', spec, conditional, config, pID, 'scatterplot')

    result = []
    # stats = {}
    if agg:
        group_obj = cond_df.groupby(x)
        finalSeries = group_obj.size()

        result = []
        for row in finalSeries.iteritems():
            # TODO General sanitation method
            try:
                cleaned_x = str(row[0])  #.translate(None, '?s.'))
                if (spec['x']['type'] != "datetime") :
                    cleaned_x = float(cleaned_x)
                result.append({
                    x: cleaned_x,
                    'count': np.asscalar(np.int16(row[1])),
                    'id': agg
                })

            except:
                pass
    else:
        y = spec['y']['title']
        result = [ {x: x_val, y: y_val} for (x_val, y_val) in zip(cond_df[x], cond_df[y]) ]

    return result

def getConditionalData(spec, dID, pID):
    df = get_data(pID=pID, dID=dID)

    if spec['name'] in ['Start Date', 'End Date']:
        time_series_data = detect_time_series(df)
        return time_series_data['time_series']['names']

    unique_elements = sorted([e for e in pd.Series(df[spec['name']]).dropna().unique()])
    return unique_elements