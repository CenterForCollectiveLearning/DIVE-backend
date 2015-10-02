import os
import shutil

from flask import make_response, jsonify, current_app
from flask.ext.restful import Resource, reqparse, marshal_with

from dive.db import db_access
from dive.resources.utilities import format_json

import logging
logger = logging.getLogger(__name__)

projectPutParser = reqparse.RequestParser()
projectPutParser.add_argument('title', type=str, required=False)
projectPutParser.add_argument('description', type=str, required=False)
class Project(Resource):
    '''
    Single Project endpoints given a project_id
    GET data for one project
    PUT data for one project
    DELETE one project
    '''
    def get(self, project_id):
        result = db_access.get_project(project_id)
        return jsonify(format_json(result))

    def put(self, project_id):
        args = projectPutParser.parse_args()
        title = args.get('title')
        description = args.get('description')
        result = db_access.update_project(project_id, title=title, description=description)
        return jsonify(format_json(result))

    def delete(self, project_id):
        result = db_access.delete_project(project_id)
        project_dir = os.path.join(current_app.config['UPLOAD_DIR'], result['id'])
        if os.path.isdir(project_dir):
            shutil.rmtree(project_dir)
        return jsonify(format_json({"message": "Successfully deleted project.",
                            "id": int(result['id'])}))

projectsGetParser = reqparse.RequestParser()
projectsGetParser.add_argument('preloaded', type=str, required=False)

projectsPostParser = reqparse.RequestParser()
projectsPostParser.add_argument('title', type=str, required=False)
projectsPostParser.add_argument('description', type=str, required=False)
projectsPostParser.add_argument('userId', type=str, required=False)
projectsPostParser.add_argument('anonymous', type=str, required=False, default=False)
class Projects(Resource):
    '''
    GET list of all projects
    POST to add new projects
    '''
    def get(self):
        args = projectsGetParser.parse_args()
        query_args = {}
        preloaded = args.get('preloaded')
        if preloaded: query_args['preloaded'] = preloaded
        return jsonify(format_json({'projects': db_access.get_projects(**query_args)}))

    # Create project, initialize directories and collections
    def post(self):
        args = projectsPostParser.parse_args()
        title = args.get('title')
        description = args.get('description')
        user_id = args.get('userId')
        anonymous = args.get('anonymous')

        result = db_access.insert_project(
            title=title,
            description=description,
            user_id=user_id
        )

        logger.info("Created upload directory for project_id: %s", result['id'])
        project_dir = os.path.join(current_app.config['UPLOAD_DIR'], str(result['id']))
        if os.path.isdir(project_dir):
            os.mkdir(project_dir)

        return jsonify(format_json(result))
