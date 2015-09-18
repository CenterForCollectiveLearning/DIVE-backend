from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

from core import logger
from .utilities import format_json
from db import db_access
from data.datasets import upload_file, get_dataset_sample
from data.dataset_properties import get_dataset_properties
from analysis.analysis import compute_ontologies, get_ontologies

ALLOWED_EXTENSIONS = set(['txt', 'csv', 'tsv', 'xlsx', 'xls', 'json'])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


# File upload handler
uploadFileParser = reqparse.RequestParser()
uploadFileParser.add_argument('project_id', type=str, required=True)
class UploadFile(Resource):
    def post(self):
        ''' Saves file and returns dataset properties '''
        form_data = json.loads(request.form.get('data'))
        project_id = form_data.get('project_id').strip().strip('""')
        file = request.files.get('file')

        if file and allowed_file(file.filename):
            # Get document with metadata and some samples
            dataset_properties = upload_file(project_id, file)
            result = {
                'status': 'success',
                'datasets': dataset_properties
            }

            dataset_doc = MI.getData({"$or" : map(lambda x: {"_id" : ObjectId(x['dID'])}, dataset_properties)}, project_id)

            # compute_ontologies(project_id, data)
            # print "Done initializing ontologies"

            return make_response(jsonify(format_json(result)))
        return make_response(jsonify(format_json({'status': 'Upload failed'})))


# Datasets list retrieval
datasetsGetParser = reqparse.RequestParser()
datasetsGetParser.add_argument('project_id', type=str, required=True)
datasetsGetParser.add_argument('getStructure', type=bool, required=False, default=False)
class Datasets(Resource):
    # Get dataset descriptions or samples
    def get(self):
        args = datasetsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')
        app.logger.info("[GET] Data for project_id: %s" % project_id)

        datasets = MI.getData({}, project_id)

        data_list = []
        for d in datasets:
            dataset_data = {
                'title': d['title'],
                'filename': d['filename'],
                'dID': d['dID']
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
    def get(self, dID):
        args = datasetGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        dataset = MI.getData({'_id': ObjectId(dID)}, project_id)[0]

        response = {
            'dID': dataset['dID'],
            'title': dataset['title'],
            'details': get_dataset_sample(dataset['dID'], project_id)
        }
        return make_response(jsonify(format_json(response)))


    def delete(self, dID):
        args = datasetDeleteParser.parse_args()
        project_id = args.get('project_id')[0]

        # TODO Handle this formatting on the client side (or server side for additional safety?)
        project_id = project_id.strip().strip('"')
        dID = dID.strip().strip('"')
        return [ MI.deleteData(dID, project_id) ]


# Public Dataset retrieval
preloadedDataGetParser = reqparse.RequestParser()
preloadedDataGetParser.add_argument('dID', type=str, action='append')
preloadedDataGetParser.add_argument('sample', type=str, required=True, default='true')

# Use public dataset in project
preloadedDataPostParser = reqparse.RequestParser()
preloadedDataPostParser.add_argument('dID', type=str, action='append')
preloadedDataPostParser.add_argument('project_id', type=str, required=True, default='true')
class PreloadedDatasets(Resource):
    # Get dataset descriptions or samples
    def get(self):
        args = preloadedDataGetParser.parse_args()
        dIDs = args.get('dID')
        project_id = 'dive'
        print "[GET] PUBLIC Data", project_id, dIDs

        # Specific dIDs
        if dIDs:
            print "Requested specific dIDs:", dIDs
            dataLocations = [ MI.getData({'_id': ObjectId(dID)}, project_id) for dID in dIDs ]

        # All datasets
        else:
            print "Did not request specific dID. Returning all datasets"
            datasets = MI.getData({}, project_id)
            data_list = []
            for d in datasets:
                path = d['path']
                result.update({
                    'title': d['title'],
                    'filename': d['filename'],
                    'dID': d['dID'],
                })
                data_list.append(result)
            return make_response(jsonify(format_json({'status': 'success', 'datasets': data_list})))

    def post(self):
        args = preloadedDataPostParser.parse_args()
        dIDs = args.get('dID')
        project_id = args.get('project_id')

        # Get data for selected datasets
        formatted_dIDs = [ObjectId(dID) for dID in dIDs]
        new_dIDs = MI.usePublicDataset({'_id': {'$in': formatted_dIDs}}, project_id)
        datasets = MI.getData({'_id': {'$in': new_dIDs}}, project_id)

        compute_field_properties(project_id, datasets)
        compute_ontologies(project_id, datasets)

        data_list = []
        for d in datasets:
            # New dID
            result.update({
                'title': d['title'],
                'filename': d['filename'],
                'path': d['path'],
                'dID': d['dID']
                })
            data_list.append(result)

        return make_response(jsonify(format_json({'status': 'success', 'datasets': data_list})))
