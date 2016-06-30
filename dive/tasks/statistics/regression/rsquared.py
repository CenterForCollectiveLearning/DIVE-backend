from dive.tasks.statistics.utilities import sets_normal, difference_of_two_lists

def get_contribution_to_r_squared_data(regression_result):
    regressions_by_column = regression_result['regressions_by_column']

    considered_fields_length_to_names = {}
    fields_to_r_squared_adj = {}

    for regression_by_column in regressions_by_column:
        column_properties = regression_by_column['column_properties']
        r_squared_adj = column_properties['r_squared_adj']
        fields = regression_by_column['regressed_fields']

        if len(fields) not in considered_fields_length_to_names:
            considered_fields_length_to_names[len(fields)] = [ fields ]
        else:
            considered_fields_length_to_names[len(fields)].append(fields)
        fields_to_r_squared_adj[str(fields)] = r_squared_adj

    max_fields_length = max(considered_fields_length_to_names.keys())
    all_fields = considered_fields_length_to_names[max_fields_length][0]
    all_fields_r_squared_adj = fields_to_r_squared_adj[str(all_fields)]

    if max_fields_length <= 1:
        return

    maximum_r_squared_adj = max(fields_to_r_squared_adj.values())

    data_array = [['Field', 'Marginal R-squared']]

    try:
        all_except_one_regression_fields = considered_fields_length_to_names[max_fields_length - 1]
        for all_except_one_regression_fields in all_except_one_regression_fields:
            regression_r_squared_adj = fields_to_r_squared_adj[str(all_except_one_regression_fields)]

            marginal_field = difference_of_two_lists(all_except_one_regression_fields, all_fields)[0]
            marginal_r_squared_adj = all_fields_r_squared_adj - regression_r_squared_adj
            data_array.append([ marginal_field, marginal_r_squared_adj])
        return data_array
    except:
        return []
