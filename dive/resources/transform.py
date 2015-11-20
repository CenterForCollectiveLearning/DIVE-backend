'''
Endpoints for uploading, getting, updating, and deleting datasets
'''
import os
import json
from flask import request, make_response, jsonify
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.utilities import format_json
from dive.tasks.transformation.reduce import reduce_dataset
from dive.tasks.transformation.pivot import pivot_dataset

import logging
logger = logging.getLogger(__name__)


def object_type(j):
    return j

#####################################################################
# Endpoint creating new dataset given a subset of columns of an existing dataset
# REQUIRED INPUT: project_id, dataset_id, fieldIndicesToKeep
# OPTIONAL INPUT: newDatasetNameSuffix
# OUTPUT: new_dataset_id
#####################################################################
reducePostParser = reqparse.RequestParser()
reducePostParser.add_argument('project_id', type=str, required=True, location='json')
reducePostParser.add_argument('dataset_id', type=str, required=True, location='json')
reducePostParser.add_argument('column_ids', type=object_type, required=True, location='json')
reducePostParser.add_argument('new_dataset_name_suffix', type=object_type, location='json', default='_reduced')
class Reduce(Resource):
    def post(self):
        args = reducePostParser.parse_args()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        column_ids = args.get('column_ids')
        new_dataset_name_suffix = args.get('new_dataset_name_suffix')

        result = reduce_dataset(project_id, dataset_id, column_ids, new_dataset_name_suffix)
        return make_response(jsonify(format_json({'dataset_id': result})))


#####################################################################
# Endpoint creating new dataset given a dataset and columns to pivot
# REQUIRED INPUT: project_id, dataset_id, pivot_fields
# OPTIONAL INPUT: variable_name, value_name
# OUTPUT: new_dataset_id
#####################################################################
pivotPostParser = reqparse.RequestParser()
pivotPostParser.add_argument('project_id', type=str, required=True, location='json')
pivotPostParser.add_argument('dataset_id', type=str, required=True, location='json')
pivotPostParser.add_argument('pivot_fields', type=object_type, required=True, location='json')
pivotPostParser.add_argument('variable_name', type=str, location='json', default='variable')
pivotPostParser.add_argument('value_name', type=str, location='json', default='value')
class Pivot(Resource):
    def post(self):
        args = pivotPostParser.parse_args()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        pivot_fields = args.get('pivot_fields')
        variable_name = args.get('variable_name')
        value_name = args.get('value_name')

        result = pivot_dataset(project_id, dataset_id, column_ids, new_dataset_name_suffix)
        return make_response(jsonify(format_json({'dataset_id': result})))
