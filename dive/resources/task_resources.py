from flask import make_response, jsonify, current_app, url_for
from flask.ext.restful import Resource, reqparse, marshal_with

from dive.task_core import celery
from dive.resources.utilities import format_json

from celery import states
from celery.result import result_from_tuple

import logging
logger = logging.getLogger(__name__)

def getChainIDs(x):
    parent = x.parent
    if parent:
        return [ x.id ] + getChainIDs(parent)
    else:
        return []

class TestPipeline(Resource):
    def get(self, project_id, dataset_id):
        from dive.tasks.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline
        task = full_pipeline(dataset_id, project_id).apply_async()

        task_id = task.id
        logger.info(getChainIDs(task))
        response = jsonify({task_id: getChainIDs(task)})
        response.status_code = 202
        return response
            # {'Location': url_for(TaskResult, task_id=task_id) })

class TaskResult(Resource):
    def get(self, task_id):
        task = celery.AsyncResult(task_id)
        logger.info("STATE %s", task.state)
        import celery as cly
        logger.info(cly.registry.tasks)
        if task.state == states.PENDING:
            state = {
                'state': task.state,
                'current': 0,
                'total': 1,
                'status': task.info #.info #.get('status', states.PENDING)
            }
        elif task.state != states.FAILURE:
            state = {
                'state': task.state,
                'info': task.info,
            }
            # if 'result' in task.info:
            #     response['result'] = task.info['result']
        else:
            state = {
                'state': task.state,
                'current': 1,
                'total': 1,
                'status': str(task.info),
            }
        response = jsonify(state)
        response.status_code = 202
        return response
