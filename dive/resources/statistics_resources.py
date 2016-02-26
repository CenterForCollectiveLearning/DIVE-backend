import time
from flask import current_app, request, make_response
from flask.ext.restful import Resource, reqparse


from dive.db import db_access
from dive.resources.serialization import replace_unserializable_numpy, jsonify
from dive.tasks.statistics.regression import run_regression_from_spec, save_regression, get_contribution_to_r_squared_data
from dive.tasks.statistics.summary import run_comparison_from_spec, get_variable_summary_statistics_from_spec, run_numerical_comparison_from_spec, create_one_dimensional_contingency_table_from_spec, create_contingency_table_from_spec
from dive.tasks.statistics.correlation import run_correlation_from_spec, get_correlation_scatterplot_data, save_correlation

import logging
logger = logging.getLogger(__name__)

#####################################################################
# Endpoint returning estimated time for regression
# INPUT: numInputs, sizeArray, funcArraySize
# OUTPUT: time
#####################################################################
# For inferred visualizations
timeFromParamsPostParser = reqparse.RequestParser()
timeFromParamsPostParser.add_argument('numInputs', type=int, location='json')
timeFromParamsPostParser.add_argument('sizeArray', type=int, location='json')
timeFromParamsPostParser.add_argument('funcArraySize', type=int, location='json')
class RegressionEstimator(Resource):
    def post(self):
        args = request.json
        # TODO Implement required parameters
        numInputs = args.get('numInputs')
        sizeArray = args.get('sizeArray')
        funcArraySize = args.get('funcArraySize')

        result, status = timeEstimator(numInputs, sizeArray, funcArraySize)
        return make_response(jsonify(result))


#####################################################################
# Endpoint returning regression data given a specification
# INPUT: project_id, spec
# OUTPUT: {stat data}
#####################################################################
regressionPostParser = reqparse.RequestParser()
regressionPostParser.add_argument('projectId', type=str, location='json')
regressionPostParser.add_argument('spec', type=str, location='json')
class RegressionFromSpec(Resource):
    def post(self):
        '''
        spec: {
            independentVariables
            dependentVariable
            model
            estimator
            degree
            weights
            functions
            datasetId
        }
        '''
        args = request.get_json()
        project_id = args.get('projectId')
        spec = args.get('spec')

        regression_doc = db_access.get_regression_from_spec(project_id, spec)
        if regression_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            regression_data = regression_doc['data']
            regression_data['id'] = regression_doc['id']
        else:
            regression_data, status = run_regression_from_spec(spec, project_id)
            serializable_regression_data = replace_unserializable_numpy(regression_data)
            regression_doc = save_regression(spec, serializable_regression_data, project_id)
            regression_data['id'] = regression_doc['id']

        logger.info(regression_data)
        return make_response(jsonify(regression_data))


contributionToRSquaredGetParser = reqparse.RequestParser()
contributionToRSquaredGetParser.add_argument('projectId', type=str)
class ContributionToRSquared(Resource):
    def get(self, regression_id):
        args = contributionToRSquaredGetParser.parse_args()
        project_id = args.get('projectId')
        regression_doc = db_access.get_regression_by_id(regression_id, project_id)
        regression_data = regression_doc['data']
        data = get_contribution_to_r_squared_data(regression_data)
        logger.info(data)
        return make_response(jsonify({ 'data': data }))


class NumericalComparisonFromSpec(Resource):
    def post(self):
        '''
        spec: {
            variable_names : list names
            dataset_id : integer
            independence : boolean
        }
        '''
        args = request.get_json()
        project_id = args.get('projectId')
        spec = args.get('spec')
        result, status = run_numerical_comparison_from_spec(spec, project_id)
        return make_response(jsonify(result), status)

class SummaryStatsFromSpec(Resource):
    def post(self):
        '''
        spec: {
            datasetId : integer
            fieldIds : list
        }
        '''
        args = request.get_json()
        project_id = args.get('projectId')
        spec = args.get('spec')
        result, status = get_variable_summary_statistics_from_spec(spec, project_id)
        return make_response(jsonify(result), status)

class OneDimensionalTableFromSpec(Resource):
    def post(self):
        '''
        spec: {
            dataset_id
            categoricalIndependentVariableNames
            numericalIndependentVariableNames
            dependentVariable
        }
        '''
        args = request.get_json()
        project_id = args.get('projectId')
        spec = args.get('spec')
        result, status = create_one_dimensional_contingency_table_from_spec(spec, project_id)
        return make_response(jsonify(result), status)

class ContingencyTableFromSpec(Resource):
    def post(self):
        '''
        spec: {
            datasetId
            categoricalIndependentVariableNames
            numericalIndependentVariableNames
            dependentVariable
        }
        '''
        args = request.get_json()
        project_id = args.get('projectId')
        spec = args.get('spec')
        result, status = create_contingency_table_from_spec(spec, project_id)
        return make_response(jsonify(result), status)

class CorrelationsFromSpec(Resource):
    def post(self):
        '''
        spec: {
            datasetId
            correlationVariables
        }
        '''
        args = request.get_json()
        project_id = args.get('projectId')
        spec = args.get('spec')

        correlation_doc = db_access.get_correlation_from_spec(project_id, spec)
        if correlation_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            correlation_data = correlation_doc['data']
            correlation_data['id'] = correlation_doc['id']
        else:
            correlation_data, status = run_correlation_from_spec(spec, project_id)
            serializable_correlation_data = replace_unserializable_numpy(correlation_data)
            correlation_doc = save_correlation(spec, serializable_correlation_data, project_id)
            correlation_data['id'] = correlation_doc['id']

        return make_response(jsonify(correlation_data))


correlationScatterplotGetParser = reqparse.RequestParser()
correlationScatterplotGetParser.add_argument('projectId', type=str)
class CorrelationScatterplot(Resource):
    def get(self, correlation_id):
        args = correlationScatterplotGetParser.parse_args()
        project_id = args.get('projectId')
        correlation_doc = db_access.get_correlation_by_id(correlation_id, project_id)
        correlation_spec = correlation_doc['spec']
        data = get_correlation_scatterplot_data(correlation_spec, project_id)
        return make_response(jsonify({ 'data': data }))


class ComparisonFromSpec(Resource):
    def post(self):
        args = request.get_json()
        project_id = args.get('project_id')
        spec = args.get('spec')
        result, status = run_comparison_from_spec(spec, project_id)
        return make_response(jsonify(result), status)


class SegmentationFromSpec(Resource):
    def post(self):
        args = request.get_json()
        project_id = args.get('project_id')
        spec = args.get('spec')
        return
