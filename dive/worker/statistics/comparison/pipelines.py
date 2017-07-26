from dive.base.db import db_access
from dive.base.data.access import get_data, get_conditioned_data

from dive.worker.statistics.comparison.numerical_comparison import run_valid_numerical_comparison_tests
from dive.worker.statistics.comparison.anova import run_anova
from dive.worker.statistics.comparison.anova_boxplot import get_anova_boxplot_data
from dive.worker.statistics.comparison.pairwise_comparison import get_pairwise_comparison_data


def run_comparison_from_spec(spec, project_id, conditionals=[]):
    dependent_variables_names = spec.get('dependentVariablesNames', [])
    independent_variables_names = spec.get('independentVariablesNames', [])  # [ iv[1] for iv in independent_variables ]
    dataset_id = spec.get('datasetId')
    significance_cutoff = spec.get('significanceCutoff', 0.05)
    independence = spec.get('independence', True)

    if not (dataset_id): return 'Not passed required parameters', 400

    all_fields = db_access.get_field_properties(project_id, dataset_id)
    dependent_variables = [ f for f in all_fields if f['name'] in dependent_variables_names ]
    independent_variables = [ f for f in all_fields if f['name'] in independent_variables_names ]

    can_run_numerical_comparison_independent = len([ iv for iv in independent_variables if iv['scale'] == 'continuous' ]) >= 2 and len(dependent_variables_names) == 0
    can_run_numerical_comparison_dependent = len([ dv for dv in dependent_variables if dv['scale'] == 'continuous' ]) >= 2 and len(independent_variables_names) == 0
    can_run_numerical_comparison = (can_run_numerical_comparison_dependent or can_run_numerical_comparison_independent)

    can_run_anova = (len(dependent_variables) and len(independent_variables))

    df = get_data(project_id=project_id, dataset_id=dataset_id)
    df_conditioned = get_conditioned_data(project_id, dataset_id, df, conditionals)
    df_subset = df_conditioned[ dependent_variables_names + independent_variables_names ]
    df_ready = df_subset.dropna(how='any')  # Remove unclean
    
    result = {}
    if can_run_anova:
        anova = run_anova(df_ready, independent_variables_names, dependent_variables_names)
        anova_boxplot_data = get_anova_boxplot_data(project_id, dataset_id, df_ready, independent_variables_names, dependent_variables_names)
        pairwise_comparison_data = get_pairwise_comparison_data(df_ready, independent_variables_names, dependent_variables_names, significance_cutoff=significance_cutoff)
        result.update({
            'anova': anova,
            'anova_boxplot': anova_boxplot_data,
            'pairwise_comparison': pairwise_comparison_data,
        })

    if can_run_numerical_comparison:
        if can_run_numerical_comparison_independent:
            numerical_comparison_data = run_valid_numerical_comparison_tests(df_ready, independent_variables_names, independence=True)
        if can_run_numerical_comparison_dependent:
            numerical_comparison_data = run_valid_numerical_comparison_tests(df_ready, dependent_variables_names, independence=False)
        result['numerical_comparison'] = numerical_comparison_data

    return result, 200

def save_comparison(spec, result, project_id, conditionals=[]):
    existing_comparison_doc = db_access.get_comparison_from_spec(project_id, spec, conditionals=conditionals)
    if existing_comparison_doc:
        db_access.delete_comparison(project_id, existing_comparison_doc['id'], conditionals=conditionals)
    inserted_comparison = db_access.insert_comparison(project_id, spec, result, conditionals=conditionals)
    return inserted_comparison
