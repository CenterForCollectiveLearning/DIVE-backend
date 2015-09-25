from flask import make_response, jsonify, current_app, url_for
from flask.ext.restful import Resource, reqparse, marshal_with

from dive.task_core import celery
from dive.resources.utilities import format_json

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
        return jsonify({'task_id': getChainIDs(task)})
            # {'Location': url_for(TaskResult, task_id=task_id) })

class TaskResult(Resource):
    def get(self, task_id):
        task = celery.AsyncResult(task_id)
        logger.info("STATE %s", task.state)
        import celery as cly
        logger.info(cly.registry.tasks)
        if task.state == 'PENDING':
            response = {
                'state': task.state,
                'current': 0,
                'total': 1,
                'status': task.info #.info #.get('status', 'Pending')
            }
        elif task.state != 'FAILURE':
            response = {
                'state': task.state,
                'info': task.result
#                'current': task.info, #.get('current', 0),
#                'total': task.info, #.get('total', 1),
#                'status': task.info, #.get('status', '')
            }
            # if 'result' in task.info:
            #     response['result'] = task.info['result']
        else:
            response = {
                'state': task.state,
                'current': 1,
                'total': 1,
                'status': str(task.info),
            }
        return jsonify(response)
