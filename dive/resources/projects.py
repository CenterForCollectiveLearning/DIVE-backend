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
        return jsonify(result)

    def put(self, project_id):
        args = projectPutParser.parse_args()
        title = args.get('title')
        description = args.get('description')
        result = db_access.update_project(project_id, title=title, description=description)
        return jsonify(result)

    def delete(self, project_id):
        result = db_access.delete_project(project_id)
        project_dir = os.path.join(current_app.config['UPLOAD_DIR'], result['id'])
        if os.path.isdir(project_dir):
            shutil.rmtree(project_dir)
        return jsonify({"message": "Successfully deleted project.",
                            "id": int(result['id'])})


projectsPostParser = reqparse.RequestParser()
projectsPostParser.add_argument('title', type=str, required=False)
projectsPostParser.add_argument('description', type=str, required=False)
projectsPostParser.add_argument('userId', type=str, required=False)
projectsPostParser.add_argument('anonymous', type=bool, required=False, default=False)
class Projects(Resource):
    '''
    GET list of all projects
    POST to add new projects
    '''
    def get(self):
        return db_access.get_projects()

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

        return jsonify(result)
