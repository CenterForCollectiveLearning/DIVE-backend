from flask import make_response, jsonify, current_app, url_for
from flask.ext.restful import Resource, reqparse, marshal_with

from celery import states
from celery.result import result_from_tuple, ResultSet

from dive.worker.core import celery
from dive.base.serialization import jsonify
from dive.worker.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline

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
        state = {
            'currentTask': '',
            'state': task.state
        }

        logger.debug('%s: %s', task_id, task.state)

        if task.state == states.PENDING:
            if (task.info) and (task.info.get('desc')):
                logger.info(task.info.get('desc'))
                state['currentTask'] = task.info.get('desc'),

        elif task.state == states.SUCCESS:
            if task.info:
                state['result'] = task.info.get('result')

        elif task.state == states.FAILURE:
            if task.info:
                try:
                    state['error'] = task.info.get('error')
                except Exception as e:
                    state['error'] = 'Unknown error occurred'

        response = jsonify(state)
        if task.state == states.PENDING:
            response.status_code = 202
        elif task.state == states.FAILURE:
            response.status_code = 500
        return response
