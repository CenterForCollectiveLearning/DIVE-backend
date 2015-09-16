import time
from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

from app import logger
from .utilities import format_json
from statistics.statistics import getStatisticsFromSpec, timeEstimator

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
# Endpoint returning statistical data given a specification
# INPUT: pID, spec
# OUTPUT: {stat data}
#####################################################################

# For inferred visualizations
statsFromSpecPostParser = reqparse.RequestParser()
statsFromSpecPostParser.add_argument('dID', type=str, location='json')
statsFromSpecPostParser.add_argument('spec', type=str, location='json')
class StatisticsFromSpec(Resource):
    def post(self):
        args = request.json
        # TODO Implement required parameters
        pID = args.get('pID')
        spec = args.get('spec')

        print time.clock()

        result, status = getStatisticsFromSpec(spec, pID)
        # print format_json(result)
        print time.clock()
        return make_response(jsonify(format_json(result)), status)
