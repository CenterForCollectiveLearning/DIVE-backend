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
-
    # 2) Run test based on test parameters and arguments
    test_result = run_test(df, arguments, model=model, degree=degree, funcArray=funcArray, estimator=estimator, weights=weights)
    return {
        'stats_data': test_result,
        'params': spec
    }, 200


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
            return multiplePolyRegression(xDict, dep_vector,1, estimator, weights)

        elif model == 'pr':
            return multiplePolyRegression(xDict, dep_vector,degree, estimator, weights)

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

    if sets_normal(0.2, residuals, yList):
        validTests['ttest'] = {'testStatistic':ttest[0],'pValue':ttest[1]}

    return validTests

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
    normal = sets_normal(.25,*args)
    numDataSets = len(args)

    if numDataSets>1:
        equalVar = variations_equal(.25,*args)

    else:
        equalVar = True

    validTests = getValidTests_noregress(equalVar, independent, normal, numDataSets)
    for test in validTests:
        if numDataSets==1:
            results[test]=validTests[test](args[0], arguments.get('userInput'))

        else:
            results[test]=validTests[test](*args)

    return results


##################
#Functions to determine which tests could be run
##################

#return a boolean, if p-value less than threshold, returns false
def variations_equal(THRESHOLD, *args):
    return stats.levene(*args)[1]>THRESHOLD

#if normalP is less than threshold, not considered normal
def sets_normal(THRESHOLD, *args):
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

#######################
##Extra Functions that could curve fit given a set of data
#######################

def powerGenerator(degree):
    def pow(x):
        return np.power(x,degree)

    return pow

def tupAppend(x,y):
    return tuple(list(x)+list(y))

############
##Returns a function that fits the data given a certain threshold
##The function can only be  simplistic ( a sum of some functions in funcList)
def automaticFit(xListList,yList,threshold,weights=None):
    funcList=[(powerGenerator(1),'x'),(powerGenerator(2),'x2'),(powerGenerator(3),'x3'),(powerGenerator(4),'x4'),(powerGenerator(5),'x5'),(powerGenerator(6),'x6'),(powerGenerator(7),'x7'),(powerGenerator(8),'x8')]
    for i in range(1,len(funcList)):
        funcs=chooseN(funcList,i)
        for funcTup in funcs:
            consideredLists=[]
            for func in funcTup:
                for list in xListList:
                    consideredLists.append(func[0](list))

            if weights==None:
                model=reg_m(yList,consideredLists,'ols')

            else:
                model=reg_m(yList,consideredLists,'wls',weights)

            if model.rsquared>threshold:
                return [funcTup,model.params]

    return 'none'
####Same as automatic fit, but returns an array of all functions that satisfies threshold
def automaticFitAll(xListList,yList,threshold):
    arrayPossFuncs=[]
    funcList=[(np.sin,'sin'),(np.cos,'cos'),(np.tan,'tan'),(powerGenerator(1),'x'),(powerGenerator(2),'x2'),(powerGenerator(3),'x3'),(powerGenerator(4),'x4'),(powerGenerator(5),'x5'),(powerGenerator(6),'x6'),(powerGenerator(7),'x7'),(powerGenerator(8),'x8')]
    for i in range(1,len(funcList)):
        funcs=chooseN(funcList,i)
        for funcTup in funcs:
            consideredLists=[]
            for func in funcTup:
                for list in xListList:
                    consideredLists.append(func[0](list))

            model=reg_m(yList,consideredLists,'ols')
            if model.rsquared>threshold:
                arrayPossFuncs.append([funcTup,model.params])

    return arrayPossFuncs

####Same as automatic fit, but forces a function to be in the equation
def forceIncludeFit(xListList, yList, threshold, funcArrayTuples):
    funcList=[(np.sin,'sin'),(np.cos,'cos'),(np.tan,'tan'),(powerGenerator(1),'x'),(powerGenerator(2),'x2'),(powerGenerator(3),'x3'),(powerGenerator(4),'x4'),(powerGenerator(5),'x5'),(powerGenerator(6),'x6'),(powerGenerator(7),'x7'),(powerGenerator(8),'x8')]
    names=map(lambda x:x[1],funcList)
    if funcArrayTuples[-1][-1] in names:
        funcList=funcList[0:names.index(funcArrayTuples[-1][-1])]

    for i in range(1,len(funcList)):
        funcs=chooseN(funcList,i)
        for funcTup in funcs:
            funcTup=tupAppend(funcTup,funcArrayTuples)
            consideredLists=[]
            for func in funcTup:
                for list in xListList:
                    consideredLists.append(func[0](list))

            model=reg_m(yList,consideredLists,'ols')
            if model.rsquared>threshold:
                return [funcTup,model.params]

####Takes the output of automatic fit and turns it into an actual equation
def formatToEquation(funcTup,params):
    funcs=[]
    funcNames=[]
    for func in funcTup:
        funcs.append(func[0])
        funcNames.append(func[1])

    def equation(x):
        return sum(map(mul, reversed(params[0:-1]), map(applyFunction, [x]*len(funcs), funcs)))+params[-1]

    return equation
