import json
from flask import request, make_response, jsonify
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.utilities import format_json
from dive.data.datasets import upload_file, get_dataset_sample
from dive.data.dataset_properties import get_dataset_properties
from dive.data.analysis import compute_ontologies, get_ontologies

import logging
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = set(['txt', 'csv', 'tsv', 'xlsx', 'xls', 'json'])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


# File upload handler
uploadFileParser = reqparse.RequestParser()
uploadFileParser.add_argument('project_id', type=str, required=True)
class UploadFile(Resource):
    ''' Saves file and returns dataset properties '''
    def post(self):
        logger.info("In upload")
        form_data = json.loads(request.form.get('data'))
        logger.info(form_data)
        project_id = form_data.get('project_id').strip().strip('""')
        file = request.files.get('file')

        if file and allowed_file(file.filename):
            # Get document with metadata and some samples
            dataset_properties = upload_file(project_id, file)
            result = {
                'status': 'success',
                'datasets': dataset_properties
            }
            return make_response(jsonify(format_json(result)))
        return make_response(jsonify(format_json({'status': 'Upload failed'})))


# Datasets list retrieval
datasetsGetParser = reqparse.RequestParser()
datasetsGetParser.add_argument('project_id', type=str, required=True)
datasetsGetParser.add_argument('getStructure', type=bool, required=False, default=False)
class Datasets(Resource):
    ''' Get dataset descriptions or samples '''
    def get(self):
        args = datasetsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')
        logger.info("[GET] Data for project_id: %s" % project_id)

        datasets = db_access.get_datasets(project_id)

        data_list = []
        for d in datasets:
            dataset_data = {
                'title': d.get('title'),
                'file_name': d.get('file_name'),
                'dataset_id': d.get('id')
            }

            if args['getStructure']:
                dataset_data['details'] = get_dataset_properties(d['path'])

            data_list.append(dataset_data)

        return make_response(jsonify(format_json({'status': 'success', 'datasets': data_list})))


# Dataset retrieval, editing, deletion
datasetGetParser = reqparse.RequestParser()
datasetGetParser.add_argument('project_id', type=str, required=True)

datasetDeleteParser = reqparse.RequestParser()
datasetDeleteParser.add_argument('project_id', type=str, required=True)
class Dataset(Resource):
    # Get dataset descriptions or samples
    def get(self, dataset_id):
        args = datasetGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        dataset = db_access.get_dataset(project_id, dataset_id)

        response = {
            'dataset_id': dataset.get('id'),
            'title': dataset.get('title'),
            'details': get_dataset_sample(dataset.get('id'), project_id)
        }
        return make_response(jsonify(format_json(response)))


    def delete(self, dataset_id):
        args = datasetDeleteParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        # Delete from datasets table
        result = db_access.delete_dataset(project_id, dataset_id)

        # Delete from file ststem
        os.remove(result['path'])
        return jsonify({"message": "Successfully deleted dataset.",
                            "id": int(result['id'])})
