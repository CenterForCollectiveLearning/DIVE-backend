from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

from app import logger
from .utilities import format_json


class VisualizationFromExportedSpec(Resource):
    def post(self):
        args = request.json
        # TODO Implement required parameters
        specID = args.get('specID')
        pID = args.get('pID')
        dID = args.get('dID')
        spec = args.get('spec')
        conditional = args.get('conditional')

        result = get_viz_data_from_enumerated_spec(spec,
            dID, pID, data_formats=['visualize', 'table'])

        return make_response(jsonify(format_json(result)))


#####################################################################
# Endpoint returning exported viz specs given a pID and optionally matching a vID
#####################################################################
exportedSpecsGetParser = reqparse.RequestParser()
exportedSpecsGetParser.add_argument('pID', type=str, required=True)
exportedSpecsGetParser.add_argument('vID', type=str, required=False)
class ExportedSpecs(Resource):
    # Return all exported viz specs, grouped by category
    def get(self):
        args = exportedSpecsGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')

        exported_specs = MI.getExportedSpecs({}, pID)

        specs_by_category = {}
        for exported_doc in exported_specs:
            spec = exported_doc['spec']
            category = spec['category']
            if category not in specs_by_category :
                specs_by_category[category] = []
            specs_by_category[category].append(exported_doc)

        return make_response(jsonify(format_json({'result': specs_by_category, 'length': len(exported_specs)})))
