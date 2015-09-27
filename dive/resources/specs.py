from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.utilities import format_json
from dive.tasks.visualization import GeneratingProcedure
from dive.tasks.visualization.data import get_viz_data_from_builder_spec, get_viz_data_from_enumerated_spec


import logging
logger = logging.getLogger(__name__)


class GeneratingProcedures(Resource):
    ''' Returns a dictionary containing the existing generating procedures. '''
    def get(self):
        result = dict([(gp.name, gp.value) for gp in GeneratingProcedure])
        return make_response(jsonify(format_json(result)))


def get_viz_specs(project_id, dataset_id=None):
    ''' Get viz specs if exists and compute if doesn't exist '''

    ### TODO Fix bug with getting tons of specs when recomputing

    specs_find_doc = {}
    if dataset_id: specs_find_doc['dataset_id'] = dataset_id

    existing_specs = db_access.get_specs(project_id, dataset_id)
    logger.info("Number of existing specs: %s", len(existing_specs))
    if existing_specs and not current_app.config['RECOMPUTE_VIZ_SPECS']:
        if dataset_id:
            return existing_specs
        else:
            result = {}
            for s in existing_specs:
                dataset_id = s['dataset_id']
                if dataset_id not in result: result[dataset_id] = [s]
                else: result[dataset_id].append(s)
            return result
    else:
        logger.info("Computing viz specs")
        return compute_viz_specs(project_id, dataset_id)


specsGetParser = reqparse.RequestParser()
specsGetParser.add_argument('project_id', type=str, required=True)
specsGetParser.add_argument('dataset_id', type=str)
class Specs(Resource):
    def get(self):
        logger.info("[GET] Specs")
        args = specsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')
        dataset_id = args.get('dataset_id', None)

        specs = db_access.get_specs(project_id, dataset_id)
        return make_response(jsonify(format_json({'specs': specs})))


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

        spec = db_access.get_spec(spec_id)

        result = {
            'spec': spec,
            'visualization': get_viz_data_from_enumerated_spec(spec, project_id, data_formats=['visualize', 'table'])
        }

        return make_response(jsonify(format_json(result)))
