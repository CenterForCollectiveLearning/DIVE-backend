from flask import make_response, jsonify, request
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.utilities import format_json
from dive.tasks.visualization import GeneratingProcedure
from dive.tasks.visualization.data import get_viz_data_from_builder_spec, get_viz_data_from_enumerated_spec
from dive.tasks.pipelines import viz_spec_pipeline

import logging
logger = logging.getLogger(__name__)


class GeneratingProcedures(Resource):
    ''' Returns a dictionary containing the existing generating procedures. '''
    def get(self):
        result = dict([(gp.name, gp.value) for gp in GeneratingProcedure])
        return make_response(jsonify(format_json(result)))


from dive.tasks.visualization.specs import enumerate_viz_specs

specsGetParser = reqparse.RequestParser()

specsGetParser.add_argument('project_id', type=str, required=True, location='json')
specsGetParser.add_argument('dataset_id', type=str, required=True, location='json')
specsGetParser.add_argument('field_agg_pairs', type=str, location='json')
class Specs(Resource):
    def post(self):
        args = request.get_json()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        field_agg_pairs = args.get('field_agg_pairs')

        specs_task = viz_spec_pipeline(dataset_id, project_id, field_agg_pairs).apply_async()
        return make_response(jsonify(format_json({'task_id': specs_task.task_id})))


visualizationGetParser = reqparse.RequestParser()
visualizationGetParser.add_argument('project_id', type=str, required=True)
class Visualization(Resource):
    ''' Returns visualization and table data for a given spec'''
    def get(self, vID):
        result = {}

        args = visualizationGetParser.parse_args()
        projectTitle = args.get('projectTitle').strip().strip('"')
        # visualizations = MI.getExportedSpecs(find_doc, project_id)

        if visualizations:
            spec = visualizations[0]['spec'][0]
            dataset_id = spec['dataset_id']
            formatted_spec = spec

            viz_data = get_viz_data_from_enumerated_spec(spec,
                project_id,
                data_formats=['visualize', 'table']
            )
            result = {
                'spec': spec,
                'visualization': viz_data
            }

        return make_response(jsonify(format_json(result)))


visualizationFromSpecGetParser = reqparse.RequestParser()
visualizationFromSpecGetParser.add_argument('project_id', type=str, required=True)
class VisualizationFromSpec(Resource):
    def get(self, spec_id):
        args = visualizationFromSpecGetParser.parse_args()
        # TODO Implement required parameters
        project_id = args.get('project_id').strip().strip('"')

        spec = db_access.get_spec(spec_id, project_id)

        result = {
            'spec': spec,
            'visualization': get_viz_data_from_enumerated_spec(spec, project_id, data_formats=['visualize', 'table'])
        }

        return make_response(jsonify(format_json(result)))
