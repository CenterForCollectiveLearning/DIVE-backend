from flask import make_response, request, current_app
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.serialization import jsonify
from dive.tasks.visualization import GeneratingProcedure
from dive.tasks.visualization.data import get_viz_data_from_enumerated_spec
from dive.tasks.pipelines import viz_spec_pipeline, get_chain_IDs
from dive.tasks.handlers import error_handler

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
specPostParser.add_argument('field_agg_pairs', type=list, location='json', default={})
specPostParser.add_argument('conditionals', type=dict, location='json', default={})
specPostParser.add_argument('config', type=dict, location='json', default={})
class Specs(Resource):
    def post(self):
        args = specPostParser.parse_args()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        selected_fields = args.get('field_agg_pairs', [])
        if not selected_fields:
            selected_fields = []
        conditionals = args.get('conditionals', {})
        config = args.get('config', {})

        specs = db_access.get_specs(project_id, dataset_id, selected_fields=selected_fields, conditionals=conditionals)

        if specs and not current_app.config['RECOMPUTE_VIZ_SPECS']:
            from time import time
            start_time = time()

            result = make_response(jsonify({
                'result': specs,
                'compute': False
            }))

            logger.info('Formatting result took %.3fs', (time() - start_time))
            return result
        else:
            specs_task = viz_spec_pipeline.apply_async(
                args = [dataset_id, project_id, selected_fields, conditionals, config],
                link_error = error_handler.s()
            )
            from time import time
            start_time = time()

            result = make_response(jsonify({
                'taskId': specs_task.task_id,
                'compute': True
            }))

            logger.info('Formatting result took %.3fs', (time() - start_time))
            return result


visualizationFromSpecPostParser = reqparse.RequestParser()
visualizationFromSpecPostParser.add_argument('project_id', type=str, required=True, location='json')
visualizationFromSpecPostParser.add_argument('conditionals', type=dict, location='json', default={})
visualizationFromSpecPostParser.add_argument('config', type=dict, location='json', default={})
class VisualizationFromSpec(Resource):
    def post(self, spec_id):
        args = visualizationFromSpecPostParser.parse_args()
        project_id = args.get('project_id')
        conditionals = args.get('conditionals', {})
        config = args.get('config', {})
        spec = db_access.get_spec(spec_id, project_id)

        result = {
            'spec': spec,
            'visualization': get_viz_data_from_enumerated_spec(spec, project_id, conditionals, config, data_formats=['visualize', 'table']),
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

        return make_response(jsonify(result))
