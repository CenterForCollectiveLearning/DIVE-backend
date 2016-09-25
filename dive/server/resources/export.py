from flask.ext.restful import Resource, reqparse

from dive.base.db import db_access
from dive.base.serialization import jsonify

import logging
logger = logging.getLogger(__name__)


exportStoryToPowerpointPostParser = reqparse.RequestParser()
exportStoryToPowerpointPostParser.add_argument('content', type=dict, required=True)
class ExportStoryToPowerpoint(Resource):
    def post(self):
        args = exportStoryToPowerpointPostParser.parse_args()
        content = args.get('content')
        print content

        result = {}
        # result = db_access.insert_exported_regression(project_id, regression_id, data)
        return jsonify(result)
