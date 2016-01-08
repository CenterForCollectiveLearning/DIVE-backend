import os

from flask import make_response, jsonify, current_app
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.utilities import format_json

import logging
logger = logging.getLogger(__name__)


documentPutParser = reqparse.RequestParser()
documentPutParser.add_argument('content', type=str, required=True)
class Document(Resource):
    '''
    Single document endpoints given a document_id of an existing document
    GET content for one document
    PUT content for one document
    DELETE one document
    '''
    def get(self, document_id):
        result = db_access.get_document(document_id)
        return jsonify(format_json(result))

    def put(self, document_id):
        args = projectPutParser.parse_args()
        content = args.get('content')
        result = db_access.update_document(document_id, content)
        return jsonify(format_json(result))

    def delete(self, document_id):
        result = db_access.delete_document(document_id)
        return jsonify(format_json({"message": "Successfully deleted project.",
                            "id": int(result['id'])}))


documentPostParser = reqparse.RequestParser()
documentPostParser.add_argument('content', type=str, required=True)
class NewDocument(Resource):
    '''
    POST to add one new document
    '''
    def put(self, document_id):
        args = projectPutParser.parse_args()
        content = args.get('content')
        result = db_access.create_document(document_id, content)
        return jsonify(format_json(result))
