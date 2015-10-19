import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
from time import time
from itertools import chain, combinations
from operator import add, mul
import time
from math import log10, floor

from dive.data.access import get_data

##########
#Run non-regression tests
##Performs comparisons between different data sets
##If only one data set is sent, it requires user input for the null hypothesis/expected values

def runValidTests_noregress(df, arguments):
    independent = arguments.get('compare').get('independent')
    args = []
    for argument in arguments.get('compare').get('dataLabels'):
        args.append(df[argument].tolist())

    results={}
    normal = setsNormal(.25,*args)
    numDataSets = len(args)

    if numDataSets>1:
        equalVar = variationsEqual(.25,*args)

    else:
        equalVar = True

    validTests = getValidTests_noregress(equalVar, independent, normal, numDataSets)
    for test in validTests:
        if numDataSets==1:
            results[test]=validTests[test](args[0], arguments.get('userInput'))

        else:
            results[test]=validTests[test](*args)

    return results


def getValidTests_noregress(equalVar, independent, normal, numDataSets):
    '''
    Get comparison tests
    '''
    if numDataSets == 1:
        validTests = {'chisquare':stats.chisquare,'power_divergence':stats.power_divergence,'kstest':stats.kstest}
        if normal:
            validTests['ttest_1samp']=stats.ttest_1samp

        return validTests

    elif numDataSets == 2:
        if independent:
            validTests = {'mannwhitneyu':stats.mannwhitneyu,'kruskal':stats.kruskal, 'ks_2samp':stats.ks_2samp}
            if normal:
                validTests['ttest_ind']=stats.ttest_ind
                if equalVar:
                    validTests['f_oneway']=stats.f_oneway

            return validTests

        else:
            validTests = {'ks_2samp':stats.ks_2samp, 'wilcoxon':stats.wilcoxon}
            if normal:
                validTests['ttest_rel']=stats.ttest_rel

            return validTests

    elif numDataSets >= 3:
        if independent:
            validTests = {'kruskal':stats.kruskal}
            if normal and equalVar:
                validTests['f_oneway']=stats.f_oneway

            return validTests

        else:
            validTests = {'friedmanchisquare':stats.friedmanchisquare}
            return validTests
