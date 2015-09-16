from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

from .utilities import format_json
from data.field_properties import get_field_properties, get_entities, get_attributes, compute_field_properties

############################
# Property (begins processing on first client API call)
# Determine: types, hierarchies, uniqueness (subset of distributions), ontology, distributions
# INPUT: pID, dID
# OUTPUT: properties corresponding to that dID
############################
propertiesGetParser = reqparse.RequestParser()
propertiesGetParser.add_argument('pID', type=str, required=True)
propertiesGetParser.add_argument('dID', type=str, required=True)
class FieldProperties(Resource):
    def get(self):
        print "[GET] Properties"
        args = propertiesGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        dID = args.get('dID')

        dataset_docs = MI.getData({"_id": ObjectId(dID)}, pID)

        # Parse properties into right return format (maybe don't do on this layer)
        properties = []

        results = {
            'properties': get_field_properties(pID, dataset_docs)
        }

        return make_response(jsonify(format_json(results)))
