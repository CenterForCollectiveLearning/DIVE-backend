from flask import make_response, jsonify, current_app
from flask.ext.restful import Resource, reqparse, marshal_with

from dive.task_core import celery
from dive.resources.utilities import format_json

class TestPipeline(Resource):
    def get(self, project_id, dataset_id):
        from dive.tasks.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline
        full_task = full_pipeline(dataset_id, project_id).apply_async()
        return full_task.id

class TaskResult(Resource):
    def get(self, task_id):
        result = celery.AsyncResult(task_id)
        return jsonify(format_json({
            'state': result.state
            'result': result.get(),
        }))
