from flask import make_response
from flask.ext.restful import Resource, reqparse

import logging
import json

from dive.db import db_access
from dive.resources.utilities import format_json, jsonify


dataFromExportedRegressionGetParser = reqparse.RequestParser()
dataFromExportedRegressionGetParser.add_argument('project_id', type=str, required=True)
class DataFromExportedRegression(Resource):
    def get(self, exported_regression_id):
        args = dataFromExportedRegressionGetParser.parse_args()
        project_id = args.get('project_id')

        exported_regression = db_access.get_exported_regression(project_id, exported_regression_id)
        regression_id = exported_spec['regression_id']
        regression = db_access.get_regression_by_id(regression_id, project_id)

        return make_response(jsonify(format_json(regression['data'])))


exportedRegressionsGetParser = reqparse.RequestParser()
exportedRegressionsGetParser.add_argument('project_id', type=str, required=True)

exportedRegressionsPostParser = reqparse.RequestParser()
exportedRegressionsPostParser.add_argument('project_id', type=str, required=True, location='json')
exportedRegressionsPostParser.add_argument('regression_id', type=str, required=True, location='json')
class ExportedRegressions(Resource):
    def get(self):
        args = exportedRegressionsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        exported_regressions = db_access.get_exported_regressions(project_id)
        return make_response(jsonify(format_json({'result': exported_regressions, 'length': len(exported_regressions)})))

    def post(self):
        args = exportedRegressionsPostParser.parse_args()
        project_id = args.get('project_id')
        regression_id = args.get('regression_id')

        result = db_access.insert_exported_regression(project_id, regression_id)
        return jsonify(format_json(result))
