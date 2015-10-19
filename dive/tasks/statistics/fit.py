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
