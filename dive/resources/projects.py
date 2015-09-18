import os
import shutil

from flask import make_response, jsonify, current_app
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.utilities import format_json

import logging
logger = logging.getLogger(__name__)

class Project(Resource):
    '''
    Single Project endpoints given a project_id
    GET data for one project
    UPDATE data for one project
    DELETE one project
    '''
    def get(self, project_id):
        result = db_access.get_project(project_id)
        return jsonify(result)

    def delete(self, project_id):
        result = db_access.delete_project(project_id)
        shutil.rmtree(os.path.join(current_app.config['UPLOAD_FOLDER'], result['id']))
        return jsonify({"message": "Successfully deleted project.",
                            "id": int(result['id'])})


projectsPostParser = reqparse.RequestParser()
projectsPostParser.add_argument('title', type=str, required=True)
projectsPostParser.add_argument('description', type=str, required=False)
projectsPostParser.add_argument('username', type=str, required=False)
projectsPostParser.add_argument('anonymous', type=bool, required=False, default=False)

projectsDeleteParser = reqparse.RequestParser()
projectsDeleteParser.add_argument('project_id', type=str, action='append', required=True)
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
        username = args.get('username')
        anonymous = args.get('anonymous')

        result = db_access.insert_project(title=title, description=description)

        logger.info("Created upload directory for project_id: %s", result['id'])
        os.mkdir(os.path.join(config['UPLOAD_FOLDER'], result['id']))

        return jsonify({"message": "Successfully created project.",
                            "id": int(result['id'])})
