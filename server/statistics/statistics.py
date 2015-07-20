import pandas as pd
import numpy as np
import scipy as sc
import statsmodels.api as sm
from time import time
from itertools import chain, combinations


def getStatisticsFromSpec(spec, pID):
    # 1) Parse and validate arguments
    dID = spec.get('dID')
    test = spec.get('test')
    arguments = spec.get('arguments')
  
    if not (dID, test):
      return "Did not pass required parameters", 400
  
    # 1) Access dataset
    df = get_data(pID=pID, dID=dID)


def all_subsets(ss):
    return chain(*map(lambda x: combinations(ss, x), range(0, len(ss)+1)))


# Multivariate linear regression function
def reg_m(y, x):
    ones = np.ones(len(x[0]))
    X = sm.add_constant(np.column_stack((x[0], ones)))
    for ele in x[1:]:
        X = sm.add_constant(np.column_stack((ele, X)))
    results = sm.OLS(y, X).fit()
    return results


# Automated test
# Input: dataframe, independent column, test type
def analyse_all(df, y, test='OLS'):
	dep_vector = df[y]
	# Long dataset
	indep_df = df.drop(y, axis=1)
	all_indep_vectors = [ indep_df[c] for c in indep_df ]
	all_indep_vectors_indices = range(0, len(all_indep_vectors))

	# Iterate through all combinations of independent vectors
	for subset in all_subsets(all_indep_vectors_indices):
		if len(subset) == 0: continue
		start_time = time()
		indep_vectors = [ all_indep_vectors[i] for i in subset ]
		results = reg_m(dep_vector, indep_vectors)
		parsed_results = {
		    'rsquared': results.rsquared,
		    'rsquared_adj': results.rsquared_adj,
		    'params': results.params.tolist(),
		}
		print _result2dataframe(results)
		print subset, len(indep_vectors), time() - start_time


# Formalize structure of output
def _result2dataframe(model_result):
    """return a series containing the summary of a linear model
    All the exceding parameters will be redirected to the linear model
    """

    # create the linear model and perform the fit
    # keeps track of some global statistics
    statistics = pd.Series({'r2': model_result.rsquared,
                  'adj_r2': model_result.rsquared_adj})

    # put them togher with the result for each term
    result_df = pd.DataFrame({'params': model_result.params,
                              'pvals': model_result.pvalues,
                              'std': model_result.bse,
                              'statistics': statistics})

    # add the complexive results for f-value and the total p-value
    fisher_df = pd.DataFrame({'params': {'_f_test': model_result.fvalue},
                              'pvals': {'_f_test': model_result.f_pvalue}})
    # merge them and unstack to obtain a hierarchically indexed series
    res_series = pd.concat([result_df, fisher_df]).unstack()
    return res_series.dropna()