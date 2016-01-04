import time
from flask import current_app, request, make_response, jsonify
from flask.ext.restful import Resource, reqparse


from dive.db import db_access
from dive.resources.utilities import format_json, replace_unserializable_numpy
from dive.tasks.statistics.regression import run_regression_from_spec, save_regression, get_contribution_to_r_squared_data
from dive.tasks.statistics.comparison import run_comparison_from_spec, run_numerical_comparison_from_spec, create_contingency_table_from_spec

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
        return result


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

        return make_response(jsonify(format_json(regression_data)))


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
        return make_response(jsonify(format_json({ 'data': data })))

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
        return make_response(jsonify(format_json({"result": result})), status)
class ContingencyTableFromSpec(Resource):
    def post(self):
        '''
        dataset_id
        ind_num_variables
        ind_cat_variables
        dep_num_variable
        dep_cat_variable
        '''
        args = request.get_json()
        project_id = args.get('projectId')
        spec = args.get('spec')
        result, status = create_contingency_table_from_spec(spec, project_id)
        return make_response(jsonify(format_json({"result": result})), status)

class ComparisonFromSpec(Resource):
    def post(self):
        args = request.get_json()
        project_id = args.get('project_id')
        spec = args.get('spec')
        result, status = run_comparison_from_spec(spec, project_id)
        return make_response(jsonify(format_json({"result": result})), status)


class SegmentationFromSpec(Resource):
    def post(self):
        args = request.get_json()
        project_id = args.get('project_id')
        spec = args.get('spec')
        return
