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

        datasets = db_access.get_datasets(project_id=project_id)

        data_list = []
        for d in datasets:
            dataset_data = {
                'title': d.get('title'),
                'filename': d.get('filename'),
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
datasetDeleteParser.add_argument('project_id', type=str, action='append', required=True)
class Dataset(Resource):
    # Get dataset descriptions or samples
    def get(self, dataset_id):
        args = datasetGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        dataset = MI.getData({'_id': ObjectId(dataset_id)}, project_id)[0]

        response = {
            'dataset_id': dataset['dataset_id'],
            'title': dataset['title'],
            'details': get_dataset_sample(dataset['dataset_id'], project_id)
        }
        return make_response(jsonify(format_json(response)))


    def delete(self, dataset_id):
        args = datasetDeleteParser.parse_args()
        project_id = args.get('project_id')[0]

        # TODO Handle this formatting on the client side (or server side for additional safety?)
        project_id = project_id.strip().strip('"')
        dataset_id = dataset_id.strip().strip('"')
        return [ MI.deleteData(dataset_id, project_id) ]


# Public Dataset retrieval
preloadedDataGetParser = reqparse.RequestParser()
preloadedDataGetParser.add_argument('dataset_id', type=str, action='append')
preloadedDataGetParser.add_argument('sample', type=str, required=True, default='true')

# Use public dataset in project
preloadedDataPostParser = reqparse.RequestParser()
preloadedDataPostParser.add_argument('dataset_id', type=str, action='append')
preloadedDataPostParser.add_argument('project_id', type=str, required=True, default='true')
class PreloadedDatasets(Resource):
    # Get dataset descriptions or samples
    def get(self):
        args = preloadedDataGetParser.parse_args()
        dataset_ids = args.get('dataset_id')
        project_id = 'dive'
        print "[GET] PUBLIC Data", project_id, dataset_ids

        # Specific dataset_ids
        if dataset_ids:
            print "Requested specific dataset_ids:", dataset_ids
            dataLocations = [ MI.getData({'_id': ObjectId(dataset_id)}, project_id) for dataset_id in dataset_ids ]

        # All datasets
        else:
            print "dataset_id not request specific dataset_id. Returning all datasets"
            datasets = MI.getData({}, project_id)
            data_list = []
            for d in datasets:
                path = d['path']
                result.update({
                    'title': d['title'],
                    'filename': d['filename'],
                    'dataset_id': d['dataset_id'],
                })
                data_list.append(result)
            return make_response(jsonify(format_json({'status': 'success', 'datasets': data_list})))

    def post(self):
        args = preloadedDataPostParser.parse_args()
        dataset_ids = args.get('dataset_id')
        project_id = args.get('project_id')

        # Get data for selected datasets
        formatted_dataset_ids = [ObjectId(dataset_id) for dataset_id in dataset_ids]
        new_dataset_ids = MI.usePublicDataset({'_id': {'$in': formatted_dataset_ids}}, project_id)
        datasets = MI.getData({'_id': {'$in': new_dataset_ids}}, project_id)

        compute_field_properties(project_id, datasets)
        compute_ontologies(project_id, datasets)

        data_list = []
        for d in datasets:
            # New dataset_id
            result.update({
                'title': d['title'],
                'filename': d['filename'],
                'path': d['path'],
                'dataset_id': d['dataset_id']
                })
            data_list.append(result)

        return make_response(jsonify(format_json({'status': 'success', 'datasets': data_list})))
