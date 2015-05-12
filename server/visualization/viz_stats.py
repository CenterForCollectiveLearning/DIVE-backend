import numpy as np
import pandas as pd
import scipy
from scipy.stats import norm, mstats, skew, linregress, chisquare
from viz_data import *

from time import time

def getVisualizationStats(category, spec, conditional, config, pID):
    stats = {}

    stat_functions = {
        'time series': getTimeSeriesStats,
        'distribution': getDistributionStats,
        'shares': getSharesStats,
        'comparison': getComparisonStats
    }

    stats = stat_functions[category](spec, conditional, config, pID)
    return stats

def nCr(n,r):
    f = math.factorial
    return f(n) / f(r) / f(n-r)

def getComparisonStats(spec, conditional, config, pID):
    print "Getting comparisons stats"
    compare_attr = spec['compare']['title']
    cond_df = getRawData('comparison', spec, conditional, config, pID).fillna(0)

    groupby = 'Brand'
    unique_elements = sorted([e for e in pd.Series(cond_df[compare_attr]).dropna().unique()])

    final_stats = {}
    num_combinations = nCr(len(unique_elements), 2)
    final_stats['count'] = num_combinations

    if num_combinations > 100:
        return final_stats

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
                a_vals.append(val_a)
                b_vals.append(val_b)

        correlation, p_value = sp.stats.pearsonr(a_vals, b_vals)
        regression_obj = {
            'correlation': correlation,
            'p_value': p_value
        }

        slope, intercept, r_value, p_value, std_err = sp.stats.linregress(a_vals, b_vals)
        correlation_obj = {
            'slope': slope,
            'intercept': intercept,
            'r_value': r_value,
            'p_value': p_value,
            'std_err': std_err
        }
        final_stats['%s\t%s' % (a, b)] = {
            'correlation': correlation_obj,
            'regression': regression_obj
        }

    return final_stats

def getDistributionStats(spec, conditional, config, pID):
    return {}

def getSharesStats(spec, conditional, config, pID):
    return {}

def getTimeSeriesStats(spec, conditional, config, pID):
    print "Calculating stats"
    stats = {}
    if spec.get('groupBy'):
        groupby = spec['groupBy']['title']
        cond_df = getRawData('treemap', spec, conditional, config, pID).fillna(0)
    
        grouped_df = cond_df.groupby(groupby)
        aggregated_series = grouped_df.sum().transpose()
        means = aggregated_series.mean().to_dict()
        stds = aggregated_series.std().to_dict()
        normalized_stds = {}
        for k, std in stds.iteritems():
            normalized_stds[k] = std / means[k]
    

        # stats['describe'] = dict(finalSeries.describe().to_dict().items() + cond_df[groupby].describe().to_dict().items())
        stats['count'] = len(np.unique(cond_df[groupby]))
        stats['means'] = means
        stats['stds'] = normalized_stds
    else:
        print "No groupBy"
        cond_df = getRawData('treemap', spec, conditional, config, pID).fillna(0)
        aggregated_series = cond_df.sum(numeric_only=True).transpose()
        mean = aggregated_series.mean()
        std = aggregated_series.std()
        normalized_std = std / mean
    
        stats = {}
        stats['count'] = 1
        stats['means'] = {'All': mean}
        stats['stds'] = {'All': normalized_std}
    return stats

def getTreemapStats(pID, spec, raw_data) :
    cond_df = raw_data.dropna()
    groupby = spec['groupBy']['title']

    stats = {}

    group_obj = cond_df.groupby(groupby)
    finalSeries = group_obj.size()

    # print "TREEMAP STATS"
    # print finalSeries.values
    # print finalSeries.describe().to_dict()
    # print cond_df[groupby].describe().to_dict()

    chisq = chisquare(finalSeries.values)
    stats['chisq'] = {
        'chisq' : chisq[0],
        'p' : chisq[1]
    }

    stats['describe'] = dict(finalSeries.describe().to_dict().items() + cond_df[groupby].describe().to_dict().items())

    # print finalSeries.shape
    stats['count'] = finalSeries.shape[0]
    return stats

def getScatterplotStats(pID, spec, raw_data) :
    agg = spec['aggregation']
    x = spec['x']['title']
    cond_df = raw_data.dropna()

    stats = {}

    if agg :
        group_obj = cond_df.groupby(x)
        finalSeries = group_obj.size()

        x_vals = cond_df[x].values
        # print finalSeries.shape
        stats['count'] = finalSeries.shape[0]

        ## descriptive stats
        stats['describe'] = cond_df[x].describe().to_dict()

        # print finalSeries
        # print "TIME SERIES? ", cond_df[x].is_time_series

        ## OTHERWISE ITS A TIME SERIES VHAT TO DO YO?!
        if (type(finalSeries.index[0]) != str) :
            # linear regression
            lin = linregress(finalSeries.index, finalSeries.values)
            stats['linregress'] = {
                'slope' : lin[0],
                'intercept' : lin[1],
                'r' : lin[2], 
                'p' : lin[3],
                'std_err' : lin[4]
            }

            # normal test
            norm_fit = norm.fit(x_vals)
            gaussian_test = mstats.normaltest(x_vals)
            skewness = skew(x_vals)
            stats['gaussian'] = {
                'mean' : norm_fit[0],
                'std' : norm_fit[1],
                'p' : gaussian_test[1],
                'skewness' : skewness
            }

        else :
            print "TIME SERIES HOHOHO"

    return stats