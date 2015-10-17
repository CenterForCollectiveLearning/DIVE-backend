from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

import logging
import json

from dive.db import db_access
from dive.resources.utilities import format_json


visualizationFromExportedSpecGetParser = reqparse.RequestParser()
visualizationFromExportedSpecGetParser.add_argument('project_id', type=str, required=True)
class VisualizationFromExportedSpec(Resource):
    def get(self, exported_spec_id):
        args = visualizationFromExportedSpecGetParser.parse_args()
        project_id = args.get('project_id')

        exported_spec = db_access.get_exported_spec(project_id, exported_spec_id)
        spec_id = exported_spec['spec_id']
        spec = db_access.get_spec(spec_id, project_id)

        return make_response(jsonify(format_json(spec)))


exportedSpecsGetParser = reqparse.RequestParser()
exportedSpecsGetParser.add_argument('project_id', type=str, required=True)

exportedSpecsPostParser = reqparse.RequestParser()
exportedSpecsPostParser.add_argument('project_id', type=str, required=True, location='json')
exportedSpecsPostParser.add_argument('spec_id', type=str, required=True, location='json')
exportedSpecsPostParser.add_argument('conditionals', type=str, required=True, location='json')
exportedSpecsPostParser.add_argument('config', type=str, required=True, location='json')
class ExportedSpecs(Resource):
    def get(self):
        args = exportedSpecsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        exported_specs = db_access.get_exported_specs(project_id)
        return make_response(jsonify(format_json({'result': exported_specs, 'length': len(exported_specs)})))

    def post(self):
        args = exportedSpecsPostParser.parse_args()
        project_id = args.get('project_id')
        spec_id = args.get('spec_id')
        conditionals = json.loads(args.get('conditionals'))
        config = json.loads(args.get('config'))

        result = db_access.insert_exported_spec(project_id, spec_id, conditionals, config)
        return jsonify(format_json(result))
