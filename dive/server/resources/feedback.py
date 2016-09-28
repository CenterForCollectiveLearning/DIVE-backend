from flask import request, make_response
from flask.ext.restful import Resource, reqparse
from flask.ext.login import login_required

from dive.base.db import db_access
from dive.base.serialization import jsonify

import logging
logger = logging.getLogger(__name__)


feedbackPostParser = reqparse.RequestParser()
feedbackPostParser.add_argument('project_id', type=str, location='json')
feedbackPostParser.add_argument('user_id', type=str, location='json')
feedbackPostParser.add_argument('user_email', type=str, location='json')
feedbackPostParser.add_argument('username', type=str, location='json')
feedbackPostParser.add_argument('feedback_type', type=str, location='json')
feedbackPostParser.add_argument('description', type=str, location='json')
class Feedback(Resource):
    @login_required
    def post(self):
        args = feedbackPostParser.parse_args()
        project_id = args.get('project_id')
        user_id = args.get('user_id')
        user_email = args.get('user_email')
        user_username = args.get('username')
        feedback_type = args.get('feedback_type')
        description = args.get('description')

        feedback = db_access.submit_feedback(project_id, user_id, user_email, user_username, feedback_type, description)
        return jsonify({
            'message': 'Feedback Received',
            'feedback_id': feedback['id']
        })
