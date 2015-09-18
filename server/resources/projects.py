import os
import shutil

from flask import make_response, jsonify
from flask.ext.restful import Resource, reqparse

from core import logger, config
from db import db_access
from .utilities import format_json


############################
# Single Project
# GET data for one project
# UPDATE data for one project
# DELETE one project
############################
class Project(Resource):
    def get(self, project_id):
        result = db_access.get_project(project_id)
        return jsonify(result)

    def delete(self, project_id):
        result = db_access.delete_project(project_id)
        return jsonify({"message": "Successfully deleted project.",
                            "id": int(result.id)})


############################
# Projects
# GET list of all projects
# POST to add new projects
############################
projectsGetParser = reqparse.RequestParser()
projectsGetParser.add_argument('user_name', type=str)

projectsPostParser = reqparse.RequestParser()
projectsPostParser.add_argument('title', type=str, required=True)
projectsPostParser.add_argument('description', type=str, required=False)
projectsPostParser.add_argument('user_name', type=str, required=False)
projectsPostParser.add_argument('anonymous', type=bool, required=False, default=False)

projectsDeleteParser = reqparse.RequestParser()
projectsDeleteParser.add_argument('project_id', type=str, action='append', required=True)
class Projects(Resource):
    def get(self):
        args = projectsGetParser.parse_args()
        return db_access.get_projects()

    # Create project, initialize directories and collections
    def post(self):
        args = projectsPostParser.parse_args()
        title = args.get('title')
        description = args.get('description')
        user_name = args.get('user_name')
        anonymous = args.get('anonymous')

        db_access.create_project(title=title)
        logger.info("Created upload directory for project_id: %s", result[0]['project_id'])
        os.mkdir(os.path.join(app.config['UPLOAD_FOLDER'], result[0]['project_id']))

        return

    def put(self):
        args = projectsPostParser.parse_args()
        title = args.get('title')
        description = args.get('description')
        user_name = args.get('user_name')
        anonymous = args.get('anonymous')

        db_access.create_project(title=title)
        logger.info("Created upload directory for project_id: %s", result[0]['project_id'])
        os.mkdir(os.path.join(app.config['UPLOAD_FOLDER'], result[0]['project_id']))

        return

    # Delete project and all associated data
    def delete(self):
        args = projectsDeleteParser.parse_args()
        project_ids = args.get('project_id')

        db_access.delete_projects(project_ids=project_ids)

        # Delete uploads directory
        for project_id in project_ids:
            shutil.rmtree(os.path.join(app_config['UPLOAD_FOLDER'], project_id))

        return
