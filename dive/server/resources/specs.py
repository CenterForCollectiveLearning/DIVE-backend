from flask import request, current_app
from flask_restful import Resource, reqparse

from flask_login import login_required

from dive.base.db import db_access
from dive.base.serialization import jsonify
from dive.worker.visualization.constants import GeneratingProcedure
from dive.worker.visualization.data import get_viz_data_from_enumerated_spec
from dive.worker.pipelines import viz_spec_pipeline
from dive.worker.handlers import error_handler

import logging
logger = logging.getLogger(__name__)


class GeneratingProcedures(Resource):
    ''' Returns a dictionary containing the existing generating procedures. '''
    def get(self):
        result = dict([(gp.name, gp.value) for gp in GeneratingProcedure])
        return make_response(jsonify(result))


specPostParser = reqparse.RequestParser()
specPostParser.add_argument('project_id', type=str, required=True, location='json')
specPostParser.add_argument('dataset_id', type=str, required=True, location='json')
specPostParser.add_argument('field_agg_pairs', type=list, location='json', default=[])
specPostParser.add_argument('recommendation_types', type=list, location='json', default=[])
specPostParser.add_argument('conditionals', type=dict, location='json', default={})
specPostParser.add_argument('config', type=dict, location='json', default={})
class Specs(Resource):
    @login_required
    def post(self):
        args = specPostParser.parse_args()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        selected_fields = args.get('field_agg_pairs', [])
        if not selected_fields:
            selected_fields = []
        recommendation_types = args.get('recommendation_types', [])
        conditionals = args.get('conditionals', {})
        config = args.get('config', {})

        specs = db_access.get_specs(project_id, dataset_id, recommendation_types=recommendation_types, selected_fields=selected_fields, conditionals=conditionals)

        if specs and not current_app.config['RECOMPUTE_VIZ_SPECS']:
            return jsonify({
                'result': specs,
                'compute': False
            })
        else:
            specs_task = viz_spec_pipeline.apply_async(
                args = [dataset_id, project_id, selected_fields, recommendation_types, conditionals, config],
                link_error = error_handler.s()
            )

            return jsonify({
                'taskId': specs_task.task_id,
                'compute': True
            })


visualizationFromSpecPostParser = reqparse.RequestParser()
visualizationFromSpecPostParser.add_argument('project_id', type=str, required=True, location='json')
visualizationFromSpecPostParser.add_argument('conditionals', type=dict, location='json', default={})
visualizationFromSpecPostParser.add_argument('config', type=dict, location='json', default={})
class VisualizationFromSpec(Resource):
    '''
    If existing spec with same conditionals and config, return data.
    Else, create a new spec.
    '''
    def post(self, spec_id):
        args = visualizationFromSpecPostParser.parse_args()
        project_id = args.get('project_id')
        conditionals = args.get('conditionals', {})
        config = args.get('config', {})
        spec = db_access.get_spec(spec_id, project_id)

        viz_data = spec.get('data', None)
        if viz_data and (conditionals == spec.get('conditionals')) and (config == spec.get('config')):
            del spec['data']
        else:
            viz_data = get_viz_data_from_enumerated_spec(spec, project_id, conditionals, config, data_formats=['visualize', 'table', 'count'])

        result = {
            'spec': spec,
            'visualization': viz_data,
            'exported': False,
            'exported_spec_id': None
        }

        existing_exported_spec = db_access.get_exported_spec_by_fields(
            project_id,
            spec_id,
            conditionals=conditionals,
            config=config
        )
        if existing_exported_spec:
            result['exported'] = True
            result['exported_spec_id'] = existing_exported_spec['id']

        return jsonify(result)
