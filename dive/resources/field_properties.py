from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

import logging
from dive.resources.utilities import format_json
from dive.data.field_properties import get_field_properties, get_entities, get_attributes, compute_field_properties

############################
# Property (begins processing on first client API call)
# Determine: types, hierarchies, uniqueness (subset of distributions), ontology, distributions
# INPUT: project_id, dID
# OUTPUT: properties corresponding to that dID
############################
propertiesGetParser = reqparse.RequestParser()
propertiesGetParser.add_argument('project_id', type=str, required=True)
propertiesGetParser.add_argument('dID', type=str, required=True)
class FieldProperties(Resource):
    def get(self):
        print "[GET] Properties"
        args = propertiesGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')
        dID = args.get('dID')

        dataset_docs = MI.getData({"_id": ObjectId(dID)}, project_id)

        # Parse properties into right return format (maybe don't do on this layer)
        properties = []

        results = {
            'properties': get_field_properties(project_id, dataset_docs)
        }

        return make_response(jsonify(format_json(results)))
