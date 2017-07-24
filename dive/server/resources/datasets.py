'''
Endpoints for uploading, getting, updating, and deleting datasets
'''
import os
import json
from flask import request, make_response
from flask_restful import Resource, reqparse
from flask_login import login_required
from celery import chain

from dive.base.db import db_access
from dive.base.db.accounts import load_account, project_auth
from dive.base.exceptions import UploadTooLargeException
from dive.base.serialization import jsonify
from dive.base.data.access import get_dataset_sample, delete_dataset
from dive.worker.pipelines import full_pipeline, ingestion_pipeline, get_chain_IDs
from dive.worker.ingestion.upload import upload_file
from dive.worker.handlers import worker_error_handler

import logging
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = set(['txt', 'csv', 'tsv', 'xlsx', 'xls', 'json'])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


# File upload handler
uploadFileParser = reqparse.RequestParser()
uploadFileParser.add_argument('project_id', type=int, required=True, location='json')
class UploadFile(Resource):
    '''
    1) Saves file
    2) Triggers data ingestion tasks
    3) Returns dataset_id
    '''
    def post(self):
        form_data = json.loads(request.form.get('data'))
        project_id = form_data.get('project_id')
        file_obj = request.files.get('file')

        if file_obj and allowed_file(file_obj.filename):

            # Get dataset_ids corresponding to file if successful upload
            try:
                datasets = upload_file(project_id, file_obj)
            except UploadTooLargeException as e:
                return jsonify({
                    'status': 'error',
                    'message': str(e)
                }, status=413)
            result = {
                'status': 'success',
                'datasets': datasets
            }
            for dataset in datasets:
                ingestion_task = ingestion_pipeline.apply_async(
                    args=[ dataset['id'], project_id ]
                )
            return jsonify({
                'task_id': ingestion_task.task_id
                }, status=202)
        return jsonify({'status': 'Upload failed'})


# Datasets list retrieval
datasetsGetParser = reqparse.RequestParser()
datasetsGetParser.add_argument('project_id', type=int, required=True)
datasetsGetParser.add_argument('get_structure', type=bool, required=False, default=False)
class Datasets(Resource):
    ''' Get dataset descriptions or samples '''
    @login_required
    def get(self):
        args = datasetsGetParser.parse_args()
        project_id = args.get('project_id')

        has_project_access, auth_message = project_auth(project_id)
        if not has_project_access: return auth_message

        datasets = db_access.get_datasets(project_id, include_preloaded=True)

        data_list = []
        for d in datasets:
            dataset_data = { k: d[k] for k in [ 'title', 'file_name', 'id', 'description', 'info_url', 'tags', 'preloaded' ]}

            dataset_data['details'] = db_access.get_dataset_properties(project_id, d.get('id'))

            data_list.append(dataset_data)

        return jsonify({'status': 'success', 'datasets': data_list})


# Datasets list retrieval
preloadedDatasetsGetParser = reqparse.RequestParser()
preloadedDatasetsGetParser.add_argument('project_id', type=int, required=False)
preloadedDatasetsGetParser.add_argument('get_structure', type=bool, required=False, default=False)
class PreloadedDatasets(Resource):
    def get(self):
        args = preloadedDatasetsGetParser.parse_args()
        project_id = args.get('project_id')
        get_structure = args.get('get_structure')

        preloaded_datasets = db_access.get_preloaded_datasets(**args)

        selected_preloaded_dataset_ids = []
        if project_id:
            selected_preloaded_datasets = db_access.get_project_preloaded_datasets(project_id)
            selected_preloaded_dataset_ids = [ d['id'] for d in selected_preloaded_datasets ]

        data_list = []
        for d in preloaded_datasets:
            dataset_data = { k: d[k] for k in [ 'title', 'file_name', 'id', 'description', 'info_url', 'tags' ]}
            if dataset_data['id'] in selected_preloaded_dataset_ids:
                dataset_data['selected'] = True
            else:
                dataset_data['selected'] = False
            dataset_data['details'] = db_access.get_dataset_properties(project_id, d.get('id'))
            data_list.append(dataset_data)


        return jsonify({'status': 'success', 'datasets': data_list})


selectPreloadedDatasetGetParser = reqparse.RequestParser()
selectPreloadedDatasetGetParser.add_argument('project_id', type=int, required=True)
selectPreloadedDatasetGetParser.add_argument('dataset_id', type=int, required=True)
class SelectPreloadedDataset(Resource):
    def get(self):
        args = selectPreloadedDatasetGetParser.parse_args()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')

        preloaded_dataset = db_access.add_preloaded_dataset_to_project(project_id, dataset_id)

        if preloaded_dataset:
            return jsonify({
                'result': 'success',
                'preloaded_dataset': { k: preloaded_dataset[k] for k in [ 'title', 'file_name', 'id', 'description', 'preloaded' ]}
            })
        else:
            return jsonify({
                'result': 'failure',
            }, status=400)

deselectPreloadedDatasetGetParser = reqparse.RequestParser()
deselectPreloadedDatasetGetParser.add_argument('project_id', type=int, required=True)
deselectPreloadedDatasetGetParser.add_argument('dataset_id', type=int, required=True)
class DeselectPreloadedDataset(Resource):
    def get(self):
        args = deselectPreloadedDatasetGetParser.parse_args()
        project_id = args.get('project_id')
        dataset_id = args.get('dataset_id')

        preloaded_dataset = db_access.remove_preloaded_dataset_from_project(project_id, dataset_id)

        if preloaded_dataset:
            return jsonify({
                'result': 'success',
                'preloaded_dataset': { k: preloaded_dataset[k] for k in [ 'title', 'file_name', 'id', 'description', 'preloaded' ]}
            })
        else:
            return jsonify({
                'result': 'failure',
            }, status=400)

# Dataset retrieval, editing, deletion
datasetGetParser = reqparse.RequestParser()
datasetGetParser.add_argument('project_id', type=int, required=True)

datasetDeleteParser = reqparse.RequestParser()
datasetDeleteParser.add_argument('project_id', type=int, required=True)
class Dataset(Resource):
    # Get dataset descriptions or samples
    @login_required
    def get(self, dataset_id):
        args = datasetGetParser.parse_args()
        project_id = args.get('project_id')

        dataset = db_access.get_dataset(project_id, dataset_id)
        sample = get_dataset_sample(dataset_id, project_id)

        response = {
            'id': dataset_id,
            'title': dataset.get('title'),
            'preloaded': dataset.get('preloaded'),
            'details': sample
        }
        return jsonify(response)

    @login_required
    def delete(self, dataset_id):
        args = datasetDeleteParser.parse_args()
        project_id = args.get('project_id')

        db_result = delete_dataset(project_id, dataset_id)

        return jsonify({
            "message": "Successfully deleted dataset.",
            "id": int(db_result['id'])
        })
