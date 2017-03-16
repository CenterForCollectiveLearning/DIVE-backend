'''
Endpoints for updating fields
'''
import os
import json
from flask import request, make_response
from flask_restful import Resource, reqparse
from flask_login import login_required

from dive.base.db import db_access
from dive.base.data.access import get_data
from dive.worker.ingestion.field_properties import compute_single_field_property_nontype
from dive.worker.ingestion.constants import quantitative_types, categorical_types, temporal_types, specific_to_general_type
from dive.base.serialization import jsonify

import logging
logger = logging.getLogger(__name__)


fieldPostParser = reqparse.RequestParser()
fieldPostParser.add_argument('project_id', type=int, required=True, location='json')
fieldPostParser.add_argument('dataset_id', type=int, required=True, location='json')
fieldPostParser.add_argument('type', type=dict, location='json')
fieldPostParser.add_argument('isId', type=bool, location='json')
fieldPostParser.add_argument('color', type=str, location='json')
class Field(Resource):
    @login_required
    def post(self, field_id):
        args = fieldPostParser.parse_args()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        field_type = args.get('type')
        field_is_id = args.get('isId')
        field_color = args.get('color')

        if field_type:
            if (field_type not in quantitative_types) \
                and (field_type not in categorical_types) \
                and (field_type not in temporal_types):
                return make_response(jsonify({'status': 'Invalid field type.'}))
            general_type = specific_to_general_type[field_type]

            field_property = db_access.get_field_property(project_id, dataset_id, field_id)
            field_name = field_property['name']
            df = get_data(project_id=project_id, dataset_id=dataset_id)

            updated_properties = compute_single_field_property_nontype(field_name, df[field_name], field_type, general_type)

            field_property_document = \
                db_access.update_field_properties_type_by_id(project_id, field_id, field_type, general_type, updated_properties)

        if field_is_id != None:
            field_property_document = \
                db_access.update_field_properties_is_id_by_id(project_id, field_id, field_is_id)

        if field_color != None:
            field_property_document = \
                db_access.update_field_properties_color_by_id(project_id, field_id, field_color)

        return make_response(jsonify(field_property_document))
