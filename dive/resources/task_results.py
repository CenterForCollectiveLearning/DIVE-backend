from flask import make_response, jsonify, current_app
from flask.ext.restful import Resource, reqparse, marshal_with

from dive.task_core import celery
from dive.resources.utilities import format_json

class TaskResult(Resource):
    def get(self, task_id):
        result = celery.AsyncResult(task_id)
        if result:
            return jsonify(format_json({"result": result.get()}))
        else:
            return
