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

import logging
logger = logging.getLogger(__name__)


#####################################################################
# Endpoint creating new dataset given a subset of columns of an existing dataset
# REQUIRED INPUT: projectID, datasetID, fieldIndicesToKeep
# OPTIONAL INPUT: newDatasetNameSuffix
# OUTPUT: newDatasetID
#####################################################################
class Reduce(Resource):
    def post(self):
        args = request.json
        # TODO Implement required parameters
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')
        column_ids = args.get('column_ids')
        new_dataset_name_suffix = args.get('new_dataset_name_suffix', '_reduced')

        result = reduce_dataset(project_id, dataset_id, column_ids, new_dataset_name_suffix)
        return make_response(jsonify(format_json({'dataset_id': result})))
