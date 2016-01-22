from flask import make_response, jsonify, request, current_app
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.utilities import format_json
from dive.tasks.visualization import GeneratingProcedure
from dive.tasks.visualization.data import get_viz_data_from_enumerated_spec
from dive.tasks.pipelines import viz_spec_pipeline, get_chain_IDs

import logging
logger = logging.getLogger(__name__)


class GeneratingProcedures(Resource):
    ''' Returns a dictionary containing the existing generating procedures. '''
    def get(self):
        result = dict([(gp.name, gp.value) for gp in GeneratingProcedure])
        return make_response(jsonify(format_json(result)))


class Specs(Resource):
    def post(self):
        args = request.get_json()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        selected_fields = args.get('field_agg_pairs', [])
        if not selected_fields:
            selected_fields = []
        conditionals = args.get('conditionals', {})

        specs = db_access.get_specs(project_id, dataset_id, selected_fields=selected_fields, conditionals=conditionals)
        if specs and not current_app.config['RECOMPUTE_VIZ_SPECS']:
            return make_response(jsonify(format_json({'specs': specs})))
        else:
            specs_task = viz_spec_pipeline(dataset_id, project_id, selected_fields, conditionals).apply_async()
            return make_response(jsonify(format_json({'task_ids': get_chain_IDs(specs_task)})))


visualizationFromSpecPostParser = reqparse.RequestParser()
visualizationFromSpecPostParser.add_argument('project_id', type=str, required=True, location='json')
visualizationFromSpecPostParser.add_argument('conditionals', type=dict, location='json', default={})
class VisualizationFromSpec(Resource):
    def post(self, spec_id):
        args = visualizationFromSpecPostParser.parse_args()
        project_id = args.get('project_id')
        conditionals = args.get('conditionals', {})
        spec = db_access.get_spec(spec_id, project_id)
        result = {
            'spec': spec,
            'visualization': get_viz_data_from_enumerated_spec(spec, project_id, conditionals, data_formats=['visualize', 'table'])
        }
        return make_response(jsonify(format_json(result)))
