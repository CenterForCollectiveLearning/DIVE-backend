import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
from time import time
from itertools import chain, combinations
from operator import add, mul
from math import log10, floor

from dive.tasks.statistics.utilities import sets_normal
from dive.db import db_access
from dive.data.access import get_data

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def run_regression_from_spec(spec, project_id):
    # 1) Parse and validate arguments
    model = spec.get('model', 'lr')
    indep = spec.get('indep', [])
    dep_name = spec.get('dep')
    estimator = spec.get('estimator', 'ols')
    degree = spec.get('degree', 1)
    weights = spec.get('weights', None)
    functions = spec.get('functions', [])
    dataset_id = spec.get('dataset_id')
    fields = db_access.get_field_properties(project_id, dataset_id)

    if not (dataset_id and dep_name):
        return "Not passed required parameters", 400

    # 2) Access dataset
    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    # 3) Run test based on parameters and arguments
    regression_result = run_regression(df, fields, indep, dep_name, model=model, degree=degree, functions=functions, estimator=estimator, weights=weights)
    return {
        'data': regression_result
    }, 200


def run_regression(df, fields, indep, dep_field_name, model='lr', degree=1, functions=[], estimator='ols', weights=None):
    indep_data = {}
    if indep:
        for indep_field_name in indep:
            indep_data[indep_field_name] = df[indep_field_name]
    else:
        print "else"
        for field in fields:
            field_name = field['name']
            if (field_name is not dep_field_name) and (field['general_type'] == 'q'):
                indep_data[field_name] = df[field_name]
    dep_data = df[dep_field_name]

    if model is 'lr':
        return multiple_polynomial_regression(indep_data, dep_data, 1, estimator, weights)
    elif model is 'pr':
        return multiple_polynomial_regression(indep_data, dep_data, degree, estimator, weights)
    elif model is 'gr':
        return general_linear_regression(indep_data, dep_data, estimator, weights)
    else:
        return


def test_regression_fit(residuals, actual_y):
    '''
    Run regression tests
    Tests how well the regression line predicts the data
    '''
    predicted_y = np.array(residuals) + np.array(actual_y)

    # Non-parametric tests (chi-square and KS)
    chisquare = stats.chisquare(predicted_y, actual_y)
    kstest = stats.ks_2samp(predicted_y, actual_y)
    results = {
        'chi_square': {
            'test_statistic': chisquare[0],
            'p_value': chisquare[1]
        },
        'ks_test': {
            'test_statistic': kstest[0],
            'p_value': kstest[1]
        }
    }

    if len(set(residuals)) > 1:
        wilcoxon = stats.wilcoxon(residuals)
        results['wilcoxon'] = {
            'testStatistic': wilcoxon[0],
            'pValue': wilcoxon[1]
        }

    if sets_normal(0.2, residuals, actual_y):
        t_test_result = stats.ttest_1samp(residuals, 0)
        results['t_test'] = {
            'test_statistic':t_test_result[0],
            'p_value':t_test_result[1]
        }

    return results


########################
#Functions for running linear regression
########################
def apply_function(ele, func):
    return func(ele)


def sum_of_array(array):
    sum=[]
    for arr in array:
        sum+=arr

    return sum


def chooseN(array, number):
    theSolutions = []
    def tupleConvert(i):
        return tuple([i])

    if number == 1:
        return map(tupleConvert, array)

    def mapper(i):
        x=map(list,chooseN(array[i+1:len(array)], number-1))
        return map(add,[[array[i]]]*(len(x)), x)

    return map(tuple,sum_of_array(map(mapper,range(len(array)-number+1))))


# Multivariate linear regression function
def multivariate_linear_regression(y, x, estimator, weights=None):
    ones = np.ones(len(x[0]))
    X = sm.add_constant(np.column_stack((x[0], ones)))
    for ele in x[1:]:
        X = sm.add_constant(np.column_stack((ele, X)))

    if estimator=='ols':
        return sm.OLS(y, X).fit()

    elif estimator=='wls':
        return sm.WLS(y, X, weights).fit()

    elif estimator=='gls':
        return sm.GLS(y, X).fit()

    return None


############################
#Run general linear regression
####func array contains the array of functions consdered in the regression
####params coefficients are reversed; the first param coefficient corresponds to the last function in func array
####notice the independent vectors are given in dictionary format, egs:{'bob':[1,2,3,4,5],'mary':[1,2,3,4,5]}
def general_linear_regression(funcArray,xDict,yList, estimator, weights=None):
    regressionDict = {}
    xKeys = xDict.keys()

    regressionDict['keys'] = xKeys
    regressionDict['list'] = []
    regressionDict['sizeList'] = []

    for chooseX in range(1, len(xKeys)+1):
        chooseXKeys = chooseN(xKeys,chooseX)
        for consideredKeys in chooseXKeys:
            consideredData = []
            for key in consideredKeys:
                for func in funcArray:
                    consideredData.append(func(np.array(xDict[key])))

            consideredData = tuple(consideredData)
            model = multivariate_linear_regression(yList,consideredData,estimator,weights)

            consideredKeysString = list(consideredKeys)
            if len(consideredKeys) == 1:
                consideredKeysString = consideredKeysString[0]

            regressionDict['list'].append(str(consideredKeysString))
            regressionDict['sizeList'].append(chooseX)
            regressionDict[consideredKeysString]= {
                'params': model.params,
                'rsquared': model.rsquared,
                'f_test': model.fvalue,
                'std': model.bse,
                'stats': test_regression_fit(model.resid, yList)
            }

    regressionDict['list']=list(reversed(regressionDict['list']))
    regressionDict['sizeList']=list(reversed(regressionDict['sizeList']))
    return regressionDict


###########################
#Runs polynomial regression
def multiple_polynomial_regression(xDict,yList,degree, estimator, weights=None):
    regressionDict = {}
    xKeys = xDict.keys()
    regressionDict['list'] = []
    regressionDict['keys'] = xKeys
    regressionDict['sizeList'] = []
    for chooseX in range(1, len(xKeys)+1):
        chooseXKeys = chooseN(xKeys,chooseX)
        for consideredKeys in chooseXKeys:
            consideredData = []
            for key in consideredKeys:
                if degree == 1:
                    consideredData.append(np.array(xDict[key].tolist()))

                else:
                    for deg in range(1,degree+1):
                        consideredData.append(np.array(xDict[key].tolist())**deg)

            model = multivariate_linear_regression(yList,consideredData, estimator, weights)

            consideredKeysString = list(consideredKeys)
            if len(consideredKeys) == 1:
                consideredKeys = list(consideredKeys[0])
            consideredKeysString = str(consideredKeys)

            regressionDict['list'].append(str(consideredKeysString))
            regressionDict['sizeList'].append(chooseX)
            regressionDict[consideredKeysString]= {
                'params': model.params,
                'rsquared': model.rsquared,
                'f_test': model.fvalue,
                'std': model.bse,
                'stats': test_regression_fit(model.resid, yList)
            }

    regressionDict['list']=list(reversed(regressionDict['list']))
    regressionDict['sizeList']=list(reversed(regressionDict['sizeList']))
    return regressionDict
