from flask import make_response
from flask.ext.restful import Resource, reqparse

from dive.base.db import db_access
from dive.server.utilities import jsonify

import logging
logger = logging.getLogger(__name__)


def object_type(j):
    return j


visualizationFromExportedSpecGetParser = reqparse.RequestParser()
visualizationFromExportedSpecGetParser.add_argument('project_id', type=str, required=True)
class VisualizationFromExportedSpec(Resource):
    def get(self, exported_spec_id):
        args = visualizationFromExportedSpecGetParser.parse_args()
        project_id = args.get('project_id')

        exported_spec = db_access.get_exported_spec(project_id, exported_spec_id)
        spec_id = exported_spec['spec_id']
        spec = db_access.get_spec(spec_id, project_id)

        result = {
            'spec': spec,
            'visualization': spec['data']
        }
        return jsonify(result)


exportedSpecsGetParser = reqparse.RequestParser()
exportedSpecsGetParser.add_argument('project_id', type=str, required=True)

exportedSpecsPostParser = reqparse.RequestParser()
exportedSpecsPostParser.add_argument('project_id', type=str, required=True, location='json')
exportedSpecsPostParser.add_argument('spec_id', type=str, required=True, location='json')
exportedSpecsPostParser.add_argument('data', type=object_type, required=True, location='json')
exportedSpecsPostParser.add_argument('conditionals', type=object_type, required=True, location='json', default={})
exportedSpecsPostParser.add_argument('config', type=object_type, required=True, location='json', default={})
class ExportedSpecs(Resource):
    def get(self):
        args = exportedSpecsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        exported_specs = db_access.get_exported_specs(project_id)
        return jsonify({'result': exported_specs, 'length': len(exported_specs)})

    def post(self):
        args = exportedSpecsPostParser.parse_args()
        project_id = args.get('project_id')
        spec_id = args.get('spec_id')
        data = args.get('data')
        conditionals = args.get('conditionals')
        config = args.get('config')
        existing_exported_spec = db_access.get_exported_spec_by_fields(
            project_id,
            spec_id,
            conditionals=conditionals,
            config=config
        )
        if existing_exported_spec:
            result = {
                'result': 'Visualization already exported.'
            }
        else:
            result = db_access.insert_exported_spec(project_id, spec_id, data, conditionals, config)
        return jsonify(result)
