from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

from dive.resources.utilities import format_json
from dive.tasks.visualization import GeneratingProcedure
from dive.tasks.visualization.specs import get_viz_specs
from dive.tasks.visualization.data import get_viz_data_from_builder_spec, get_viz_data_from_enumerated_spec


import logging
logger = logging.getLogger(__name__)


class GeneratingProcedures(Resource):
    ''' Returns a dictionary containing the existing generating procedures. '''
    def get(self):
        result = dict([(gp.name, gp.value) for gp in GeneratingProcedure])
        return make_response(jsonify(format_json(result)))


specsGetParser = reqparse.RequestParser()
specsGetParser.add_argument('project_id', type=str, required=True)
specsGetParser.add_argument('dataset_id', type=str)
class Specs(Resource):
    def get(self):
        logger.info("[GET] Specs")
        args = specsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')
        dataset_id = args.get('dataset_id', None)

        specs_by_dataset_id = get_viz_specs(project_id, dataset_id)
        return make_response(jsonify(format_json({'specs': specs_by_dataset_id})))


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
            del formatted_spec['_id']

            result = {
                'spec': spec,
                'visualization': get_viz_data_from_enumerated_spec(spec, dataset_id, project_id, data_formats=['visualize', 'table'])
            }

        return make_response(jsonify(format_json(result)))


class VisualizationFromSpec(Resource):
    def post(self):
        args = request.json
        # TODO Implement required parameters
        specID = args.get('specID')
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        spec = args.get('spec')
        conditional = args.get('conditional')

        result = get_viz_data_from_enumerated_spec(spec,
            dataset_id, project_id, data_formats=['visualize', 'table'])

        return make_response(jsonify(format_json(result)))
