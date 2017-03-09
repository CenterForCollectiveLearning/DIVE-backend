import os

from flask import make_response, current_app
from flask_restful import Resource, reqparse

from dive.base.db import db_access
from dive.base.serialization import jsonify

import logging
logger = logging.getLogger(__name__)


documentsGetParser = reqparse.RequestParser()
documentsGetParser.add_argument('project_id', type=int, required=True)
class Documents(Resource):
    '''
    GET all documents
    '''
    def get(self):
        args = documentsGetParser.parse_args()
        project_id = args.get('project_id')
        result = db_access.get_documents(project_id)
        return jsonify({
            'documents': result
        })


documentGetParser = reqparse.RequestParser()
documentGetParser.add_argument('project_id', type=int)
documentGetParser.add_argument('include_data', type=bool, default=False)

documentPutParser = reqparse.RequestParser()
documentPutParser.add_argument('project_id', type=int, required=True, location='json')
documentPutParser.add_argument('title', type=str, location='json')
documentPutParser.add_argument('content', type=dict, location='json')

documentDeleteParser = reqparse.RequestParser()
documentDeleteParser.add_argument('project_id', type=int, required=True)
class Document(Resource):
    '''
    Single document endpoints given a document_id of an existing document
    GET content for one document
    PUT content for one document
    DELETE one document
    '''
    def get(self, document_id):
        args = documentGetParser.parse_args()
        project_id = args.get('project_id')
        include_data = args.get('include_data')
        document = db_access.get_public_document(document_id)

        if include_data:
            new_document = document
            new_blocks = []
            for block in document['content']['blocks']:
                new_block = block
                exported_spec_id = block['exportedSpecId']
                exported_spec_type = block['contentType']
                exported_spec = db_access.get_public_exported_spec(exported_spec_id, exported_spec_type)
                new_block['spec'] = exported_spec
                new_blocks.append(new_block)
            new_document['content']['blocks'] = new_blocks
            result = new_document
        else:
            result = document

        return jsonify(result)

    def put(self, document_id):
        args = documentPutParser.parse_args()
        content = args.get('content')
        title = args.get('title')
        project_id = args.get('project_id')
        result = db_access.update_document(project_id, document_id, title, content)
        return jsonify(result)

    def delete(self, document_id):
        args = documentDeleteParser.parse_args()
        project_id = args.get('project_id')
        result = db_access.delete_document(project_id, document_id)
        return jsonify({"message": "Successfully deleted project.",
                            "id": int(result['id'])})


documentPostParser = reqparse.RequestParser()
documentPostParser.add_argument('project_id', type=int, required=True, location='json')
class NewDocument(Resource):
    '''
    POST to add one new document
    '''
    def post(self):
        args = documentPostParser.parse_args()
        project_id = args.get('project_id')
        result = db_access.create_document(project_id)
        return jsonify(result)
