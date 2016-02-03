from flask import make_response, jsonify, current_app, url_for
from flask.ext.restful import Resource, reqparse, marshal_with

from celery import states, chain, group
from celery.result import result_from_tuple, ResultSet

from dive.task_core import celery
from dive.resources.serialization import jsonify
from dive.tasks.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline

import logging
logger = logging.getLogger(__name__)


def object_type(j):
    return j


class RevokeTask(Resource):
    def get(self, task_id):
        logger.debug('Revoking task: %s', task_id)
        r = celery.control.revoke(task_id)


revokeChainTaskPostParser = reqparse.RequestParser()
revokeChainTaskPostParser.add_argument('task_ids', type=object_type, required=True, location='json')
class RevokeChainTask(Resource):
    def post(self):
        args = revokeChainTaskPostParser.parse_args()
        task_ids = args.get('task_ids')
        logger.debug('Revoking tasks: %s', task_ids)
        celery.control.revoke(task_ids)


class TaskResult(Resource):
    '''
    Have consistent status codes
    '''
    def get(self, task_id):
        task = celery.AsyncResult(task_id)

        if task.state == states.PENDING:
            if (task.info) and (task.info.get('desc')):
                logger.info(task.info.get('desc'))
                state = {
                    'currentTask': task.info.get('desc'),
                    'state': task.state,
                }
            else:
                state = {
                    'currentTask': '',
                    'state': task.state,
                }
        elif task.state == states.SUCCESS:
            state = {
                'result': task.info.get('result'),
                'state': task.state,
            }

        response = jsonify(state)
        if task.state == states.PENDING:
            response.status_code = 202
        return response
