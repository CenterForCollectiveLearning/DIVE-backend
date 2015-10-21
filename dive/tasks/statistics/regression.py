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

############
#Note: spec is dictionary with at most keys dataset_id, model, arguments, estimator, weights, degree, funcArray
#Note: arguments is in this format {'ind':[list of vectors], 'dep':[vector], 'compare':{'indepedent': bool, 'dataLabels':[list of vectors]}}
######argument has a 'compare' field only when a statistical comparison between vectors are performed.
######argument has an 'ind' and 'dep' field only when a regression is performed
def getStatisticsFromSpec(spec, project_id):
    # 1) Parse and validate arguments
    dataset_id = spec.get('dataset_id')
    #either a regression or comparison
    model = spec.get('model')
    #arguments is dict, includes compare and dep and datalabals, dep, indep
    arguments = spec.get('arguments')
    estimator = arguments.get('estimator')
    weights = spec.get('weights')
    degree = spec.get('degree')
    funcArray = spec.get('functions')

    if not (dataset_id, model):
        return "dataset_id not pass required parameters", 400

    # 1) Access dataset
    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df = df.dropna()  # Remove unclean

    # 2) Run test based on test parameters and arguments
    test_result = run_test(df, arguments, model=model, degree=degree, funcArray=funcArray, estimator=estimator, weights=weights)
    return {
        'stats_data': test_result,
        'params': spec
    }, 200


############
#Functions that run all tests
############
def run_test(df, arguments, model='lr', degree=1, funcArray=None, estimator='ols', weights=None):
    #if no model, assumes comparison
    if model == None:
        return runValidTests_noregress(df, arguments)

    #otherwise, runs a regression
    else:
        indep_labels = arguments.get('indep')
        xDict = {}
        for label in indep_labels:
            if label!='birthyear':
                xDict[label]=df[label]

        dep_label = arguments.get('dep')
        dep_vector = df[dep_label]

        #lr=liner regression, pr=polynomial regression, gr=general regression
        if model == 'lr':
            return multiplePolyRegression(xDict, dep_vector, 1, estimator, weights)

        elif model == 'pr':
            return multiplePolyRegression(xDict, dep_vector, degree, estimator, weights)

        elif model == 'gr':
            return multipleRegression(funcArray, xDict, dep_vector, estimator, weights)

        return


##########
#Run regression tests
##Tests how well the regression line predicts the data

def runValidTests_regress(residuals, yList):
    predictedY = np.array(residuals)+np.array(yList)

    chisquare = stats.chisquare(predictedY,yList)
    kstest = stats.ks_2samp(predictedY, yList)
    wilcoxon = stats.wilcoxon(residuals)
    ttest = stats.ttest_1samp(residuals,0)

    validTests={'chisquare': {'testStatistic':chisquare[0], 'pValue':chisquare[1]}, 'kstest':{'testStatistic':kstest[0], 'pValue':kstest[1]}}
    if len(set(residuals))>1:
        validTests['wilcoxon'] = {'testStatistic':wilcoxon[0], 'pValue':wilcoxon[1]}

    if setsNormal(0.2, residuals, yList):
        validTests['ttest'] = {'testStatistic':ttest[0],'pValue':ttest[1]}

    return validTests


########################
#Functions for running linear regression
########################
def applyFunction(ele,func):
    return func(ele)

def sum2Array(array):
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

    return map(tuple,sum2Array(map(mapper,range(len(array)-number+1))))

# Multivariate linear regression function
def reg_m(y, x, estimator, weights=None):
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
def multipleRegression(funcArray,xDict,yList, estimator, weights=None):
    regressionDict = {}
    xKeys = xDict.keys()
    regressionDict['keys']=xKeys
    regressionDict['list']=[]
    regressionDict['sizeList'] = []
    for chooseX in range(1,len(xKeys)+1):
        chooseXKeys = chooseN(xKeys,chooseX)
        for consideredKeys in chooseXKeys:
            consideredData = []
            for key in consideredKeys:
                for func in funcArray:
                    consideredData.append(func(np.array(xDict[key])))

            consideredData = tuple(consideredData)
            model = reg_m(yList,consideredData,estimator,weights)
            consideredKeysString=str(consideredKeys)
            if len(consideredKeys)==1:
                consideredKeysString=consideredKeysString[0:len(consideredKeysString)-2]+')'

            regressionDict['list'].append(consideredKeysString)
            regressionDict['sizeList'].append(chooseX)
            regressionDict[consideredKeysString]={}
            regressionDict[consideredKeysString]['params']= model.params
            regressionDict[consideredKeysString]['rsquared']= model.rsquared
            regressionDict[consideredKeysString]['f_test']= model.fvalue
            regressionDict[consideredKeysString]['std']= model.bse
            regressionDict[consideredKeysString]['stats']= runValidTests_regress(model.resid, yList)

    regressionDict['list']=list(reversed(regressionDict['list']))
    regressionDict['sizeList']=list(reversed(regressionDict['sizeList']))
    return regressionDict

###########################
#Runs polynomial regression
def multiplePolyRegression(xDict,yList,degree, estimator, weights=None):
    regressionDict = {}
    xKeys = xDict.keys()
    regressionDict['list']=[]
    regressionDict['keys']=xKeys
    regressionDict['sizeList'] = []
    for chooseX in range(1,len(xKeys)+1):
        chooseXKeys = chooseN(xKeys,chooseX)
        for consideredKeys in chooseXKeys:
            consideredData = []
            for key in consideredKeys:
                if degree == 1:
                    consideredData.append(np.array(xDict[key].tolist()))

                else:
                    for deg in range(1,degree+1):
                        consideredData.append(np.array(xDict[key].tolist())**deg)

            model = reg_m(yList,consideredData, estimator, weights)
            consideredKeysString=str(consideredKeys)
            if len(consideredKeys)==1:
                consideredKeysString=consideredKeysString[0:len(consideredKeysString)-2]+')'

            regressionDict['list'].append(consideredKeysString)
            regressionDict['sizeList'].append(chooseX)
            regressionDict[consideredKeysString]={}
            regressionDict[consideredKeysString]['params']= model.params
            regressionDict[consideredKeysString]['rsquared']= model.rsquared
            regressionDict[consideredKeysString]['f_test']= model.fvalue
            regressionDict[consideredKeysString]['std']= model.bse
            regressionDict[consideredKeysString]['stats']= runValidTests_regress(model.resid, yList)

    regressionDict['list']=list(reversed(regressionDict['list']))
    regressionDict['sizeList']=list(reversed(regressionDict['sizeList']))
    return regressionDict
