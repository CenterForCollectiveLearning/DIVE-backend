

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
            'test_statistic': t_test_result[0],
            'p_value': t_test_result[1]
        }

    return results
