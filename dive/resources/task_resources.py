from flask import make_response, jsonify, current_app
from flask.ext.restful import Resource, reqparse, marshal_with

from dive.task_core import celery
from dive.resources.utilities import format_json

import logging
logger = logging.getLogger(__name__)

class TestPipeline(Resource):
    def get(self, project_id, dataset_id):
        from dive.tasks.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline
        full_pipeline_task = full_pipeline(dataset_id, project_id).apply_async()
        return full_pipeline_task.id

class TaskResult(Resource):
    def get(self, task_id):
        result = celery.AsyncResult(task_id)
        if result:
            return jsonify(format_json({"result": result.get()}))
        else:
            return
