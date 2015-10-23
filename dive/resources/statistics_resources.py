import time
from flask import request, make_response, jsonify
from flask.ext.restful import Resource, reqparse

from dive.resources.utilities import format_json
from dive.tasks.statistics.regression import run_regression_from_spec
from dive.tasks.statistics.comparison import run_comparison_from_spec

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
        result, status = run_regression_from_spec(spec, project_id)
        return make_response(jsonify(format_json(result)), status)


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
