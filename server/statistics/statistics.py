import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
from time import time
from itertools import chain, combinations

from data.access import get_data

############
#Functions that run all tests
############


############
#Note: spec is dictionary with at most keys dID, model, arguments, estimator, weights, degree, funcArray, userInput (some may not exist)
#Note: arguments is in this format {'ind':[list of vectors], 'dep':[vector], 'compare':{'indepedent': bool, 'data':[list of vectors]}}
######argument has a 'compare' field only when a statistical comparison between vectors are performed.
######argument has an 'ind' and 'dep' field only when a regression is performed

def getStatisticsFromSpec(spec, pID):
    # 1) Parse and validate arguments
    dID = spec.get('dID')
    #either a regression or comparison
    model = spec.get('model')
    #arguments is dict, includes compare and dep and datalabals, dep, indep
    arguments = spec.get('arguments')
    estimator = spec.get('estimator')
    weights = spec.get('weights')
    degree = spec.get('degree')
    funcArray = spec.get('functions')
    userInput = spec.get('userInput')

    if not (dID, model):
        return "Did not pass required parameters", 400

    # 1) Access dataset
    df = get_data(pID=pID, dID=dID)
    df = df.dropna()  # Remove unclean

    # 2) Run test based on test parameters and arguments
    test_result = run_test(df, arguments, model=model, degree=degree, funcArray=funcArray, estimator=estimator, weights=weights, userInput=userInput)


    return {
        'stats_data': test_result
    }, 200



def run_test(df, arguments, model=None, degree=1, funcArray=None, estimator='OLS', weights=None, userInput=None):

    #if no model, assumes comparison
    if model == None:
        return runValidTests_noregress(df, userInput, arguments)

    #otherwise, runs a regression
    else:
        indep_labels = arguments.get('indep')
        xDict = {}
        for label in indep_labels:
            xDict[label]=df[label]

        dep_label = arguments.get('dep')
        dep_vector = df[dep_label]

        #lr=liner regression, pr=polynomial regression, gr=general regression
        if model == 'lr':
            print "ARGS", arguments
            return multiplePolyRegression(xDict, dep_vector,1, estimator, weights)
        elif model == 'pr':
            print "ARGS", arguments
            return multiplePolyRegression(xDict, dep_vector,degree, estimator, weights)
        elif model == 'gr':
            print "ARGS", arguments
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

##########
#Run non-regression tests
##Performs comparisons between different data sets
##If only one data set is sent, it requires user input for the null hypothesis/expected values

def runValidTests_noregress(df, userInput, arguments):
    independent = arguments.get('compare').get('independent')
    args = []
    for argument in arguments.get('compare').get('data'):
        args.append(df[argument])
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
            results[test]=validTests[test](args[0], userInput)
        else:
            results[test]=validTests[test](*args)
    return results


##################
#Functions to determine which tests could be run
##################

#return a boolean, if p-value less than threshold, returns false
def variationsEqual(THRESHOLD, *args):
    return stats.levene(*args)[1]>THRESHOLD

#if normalP is less than threshold, not considered normal
def setsNormal(THRESHOLD, *args):
    normal = True;
    for arg in args:
        if stats.normaltest(arg)[1] < THRESHOLD:
            normal = False;
    return normal

def getValidTests_noregress(equalVar, independent, normal, numDataSets):
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

########################
#Functions for running linear regression
########################
def chooseN(array, number):
    theSolutions = []
    if number == 1:
        for x in array:
            theSolutions.append(tuple([x]))
        return theSolutions
    for i in range(len(array)-number+1):
        frstNumber = [[array[i]]];
        restNumber = array[i+1:]
        restNumbersTuple = chooseN(restNumber,number-1)
        restNumbersList = []
        for tupleT in restNumbersTuple:
            restNumbersList.append(list(tupleT))
        iterationNumbers = (map(add, frstNumber*len(restNumbersList), restNumbersList))
        for x in iterationNumbers:
            theSolutions.append(tuple(x))
    return theSolutions

# Multivariate linear regression function
def reg_m(y, x, typeModel, weights=None):
    ones = np.ones(len(x[0]))
    X = sm.add_constant(np.column_stack((x[0], ones)))
    for ele in x[1:]:
        X = sm.add_constant(np.column_stack((ele, X)))
    if typeModel=='OLS':
        results = sm.OLS(y, X).fit()
    elif typeModel=='WLS':
        results = sm.WLS(y, X, weights).fit()
    elif typeModel=='GLS':
        results = sm.GLS(y, X).fit()
    return results

############################
#Run general linear regression
####func array contains the array of functions consdered in the regression
####params coefficients are reversed; the first param coefficient corresponds to the last function in func array
####notice the independent vectors are given in dictionary format, egs:{'bob':[1,2,3,4,5],'mary':[1,2,3,4,5]}

def multipleRegression(funcArray,xDict,yList, typeModel, weights=None):
    regressionDict = {}
    xKeys = xDict.keys()
    for chooseX in range(1,len(xKeys)+1):
        chooseXKeys = chooseN(xKeys,chooseX)
        for consideredKeys in chooseXKeys:
            consideredData = []
            for key in consideredKeys:
                for func in funcArray:
                    consideredData.append(func(np.array(xDict[key])))
                consideredData = tuple(consideredData)
                print consideredData
            model = reg_m(yList,consideredData,typeModel,weights)
            regressionDict[consideredKeys]={}
            regressionDict[consideredKeys]['params']= model.params
            regressionDict[consideredKeys]['rsquared']= model.rsquared
            regressionDict[consideredKeys]['f_test']= model.fvalue
            regressionDict[consideredKeys]['std']= model.bse
            regressionDict[consideredKeys]['stats']= runValidTests_regress(model.resid, yList)
    return regressionDict

###########################
#Runs polynomial regression

def multiplePolyRegression(xDict,yList,degree, typeModel, weights=None):
    regressionDict = {}
    xKeys = xDict.keys()
    for chooseX in range(1,len(xKeys)+1):
        chooseXKeys = chooseN(xKeys,chooseX)
        for consideredKeys in chooseXKeys:
            consideredData = []
            for key in consideredKeys:
                for deg in range(1,degree+1):
                    consideredData.append(np.array(xDict[key])**deg)
            model = reg_m(yList,consideredData, typeModel, weights)
            regressionDict[consideredKeys]={}
            regressionDict[consideredKeys]['params']= model.params
            regressionDict[consideredKeys]['rsquared']= model.rsquared
            regressionDict[consideredKeys]['f_test']= model.fvalue
            regressionDict[consideredKeys]['std']= model.bse
            regressionDict[consideredKeys]['stats']= runValidTests_regress(model.resid, yList)
            if chooseX==1 and degree==1:
                regressionDict[consideredKeys]['theil-sen']=stats.theilslopes(yList, consideredData)
    return regressionDict
