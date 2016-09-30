'''
Endpoints for uploading, getting, updating, and deleting datasets
'''
import os
import json
from flask import request, make_response
from flask_restful import Resource, reqparse

from dive.base.db import db_access
from dive.base.serialization import jsonify
from dive.worker.pipelines import unpivot_pipeline, reduce_pipeline, join_pipeline
from dive.worker.handlers import error_handler

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
reducePostParser.add_argument('new_dataset_name_prefix', type=str, location='json', default='[REDUCED]')
class Reduce(Resource):
    def post(self):
        args = reducePostParser.parse_args()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        column_ids = args.get('column_ids')
        new_dataset_name_prefix = args.get('new_dataset_name_prefix')

        reduce_task = reduce_pipeline.apply_async(
            args = [column_ids, new_dataset_name_prefix, dataset_id, project_id],
            link_error = error_handler.s()
        )
        return make_response(jsonify({ 'taskId': reduce_task.task_id }))


#####################################################################
# Endpoint creating new dataset given a dataset and columns to unpivot
# REQUIRED INPUT: project_id, dataset_id, unpivot_fields
# OPTIONAL INPUT: variable_name, value_name
# OUTPUT: new_dataset_id
#####################################################################
unpivotPostParser = reqparse.RequestParser()
unpivotPostParser.add_argument('project_id', type=str, required=True, location='json')
unpivotPostParser.add_argument('dataset_id', type=str, required=True, location='json')
unpivotPostParser.add_argument('pivot_fields', type=object_type, required=True, location='json')
unpivotPostParser.add_argument('variable_name', type=str, location='json', default='variable')
unpivotPostParser.add_argument('value_name', type=str, location='json', default='value')
unpivotPostParser.add_argument('new_dataset_name_prefix', type=str, location='json', default='[UNPIVOTED]')
class Unpivot(Resource):
    def post(self):
        args = unpivotPostParser.parse_args()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        pivot_fields = args.get('pivot_fields')
        variable_name = args.get('variable_name')
        value_name = args.get('value_name')
        new_dataset_name_prefix = args.get('new_dataset_name_prefix')

        unpivot_task = unpivot_pipeline.apply_async(
            args = [pivot_fields, variable_name, value_name, new_dataset_name_prefix, dataset_id, project_id],
            link_error = error_handler.s()
        )
        return make_response(jsonify({ 'taskId': unpivot_task.task_id }))


#####################################################################
# Endpoint creating new dataset given a dataset and columns to unpivot
# REQUIRED INPUT: project_id, dataset_id, unpivot_fields
# OPTIONAL INPUT: variable_name, value_name
# OUTPUT: new_dataset_id
#####################################################################
joinPostParser = reqparse.RequestParser()
joinPostParser.add_argument('project_id', type=str, required=True, location='json')
joinPostParser.add_argument('left_dataset_id', type=str, required=True, location='json')
joinPostParser.add_argument('right_dataset_id', type=str, required=True, location='json')
joinPostParser.add_argument('on', type=object_type, location='json', default=None)
joinPostParser.add_argument('left_on', type=object_type, location='json', default=None)
joinPostParser.add_argument('right_on', type=object_type, location='json', default=None)
joinPostParser.add_argument('how', type=object_type, location='json', default='inner')
joinPostParser.add_argument('sort', type=bool, location='json', default=False)
joinPostParser.add_argument('left_suffix', type=str, location='json', default='_left')
joinPostParser.add_argument('right_suffix', type=str, location='json', default='_right')
joinPostParser.add_argument('new_dataset_name_prefix', type=str, location='json', default='[JOINED]')
class Join(Resource):
    def post(self):
        args = joinPostParser.parse_args()
        project_id = args.get('project_id')
        left_dataset_id = args.get('left_dataset_id')
        right_dataset_id = args.get('right_dataset_id')
        on = args.get('on')
        left_on = args.get('left_on')
        right_on = args.get('right_on')
        how = args.get('how')
        left_suffix = args.get('left_suffix')
        right_suffix = args.get('right_suffix')
        new_dataset_name_prefix = args.get('new_dataset_name_prefix')

        join_task = join_pipeline.apply_async(args=[
            left_dataset_id, right_dataset_id, on, left_on, right_on, how,
            left_suffix, right_suffix, new_dataset_name_prefix, project_id
        ],
        link_error = error_handler.s())

        return make_response(jsonify({ 'taskId': join_task.task_id }))
