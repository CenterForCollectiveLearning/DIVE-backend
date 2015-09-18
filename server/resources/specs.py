from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

from app import logger
from .utilities import format_json
from visualization import GeneratingProcedure
from visualization.viz_specs import get_viz_specs
from visualization.viz_data import get_viz_data_from_builder_spec, get_viz_data_from_enumerated_spec


class GeneratingProcedures(Resource):
    ''' Returns a dictionary containing the existing generating procedures. '''
    def get(self):
        result = dict([(gp.name, gp.value) for gp in GeneratingProcedure])
        return make_response(jsonify(format_json(result)))


specsGetParser = reqparse.RequestParser()
specsGetParser.add_argument('project_id', type=str, required=True)
specsGetParser.add_argument('dID', type=str)
class Specs(Resource):
    def get(self):
        print "[GET] Specs"
        args = specsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')
        dID = args.get('dID', None)

        specs_by_dID = get_viz_specs(project_id, dID)

        return make_response(jsonify(format_json({'specs': specs_by_dID})))


visualizationGetParser = reqparse.RequestParser()
visualizationGetParser.add_argument('projectTitle', type=str, required=True)
class Visualization(Resource):
    ''' Returns visualization and table data for a given spec'''
    def get(self, vID):
        result = {}

        args = visualizationGetParser.parse_args()
        projectTitle = args.get('projectTitle').strip().strip('"')

        project_id = MI.getProjectID(projectTitle)

        find_doc = {'_id': ObjectId(vID)}
        visualizations = MI.getExportedSpecs(find_doc, project_id)

        if visualizations:
            spec = visualizations[0]['spec'][0]
            dID = spec['dID']
            formatted_spec = spec
            del formatted_spec['_id']

            result = {
                'spec': spec,
                'visualization': get_viz_data_from_enumerated_spec(spec, dID, project_id, data_formats=['visualize', 'table'])
            }

        return make_response(jsonify(format_json(result)))


class VisualizationFromSpec(Resource):
    def post(self):
        args = request.json
        # TODO Implement required parameters
        specID = args.get('specID')
        project_id = args.get('project_id')
        dID = args.get('dID')
        spec = args.get('spec')
        conditional = args.get('conditional')

        result = get_viz_data_from_enumerated_spec(spec,
            dID, project_id, data_formats=['visualize', 'table'])

        return make_response(jsonify(format_json(result)))
