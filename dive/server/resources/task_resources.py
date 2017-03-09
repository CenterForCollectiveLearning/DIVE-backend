from flask import make_response, jsonify, current_app, url_for
from flask_restful import Resource, reqparse, marshal_with

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

        # TODO Terminate or not?
        r = celery.control.revoke(task_ids, terminate=False)


class TaskResult(Resource):
    '''
    Have consistent status codes
    '''
    def get(self, task_id):
        task = celery.AsyncResult(task_id)
        result = {
            'currentTask': '',
            'state': task.state
        }
        state = task.state

        if state == states.PENDING:
            try:
                if (task.info) and (task.info.get('desc')):
                    result['currentTask'] = task.info.get('desc')
            except AttributeError:
                if (task.info):
                    state = states.FAILURE
                    result['state'] = states.FAILURE
                    result['currentTask'] = task.info
            logger.info('PENDING %s: %s', task_id, state)

        elif state == states.SUCCESS:
            if task.info:
                result['result'] = task.info.get('result')

        elif state == states.FAILURE:
            if task.info:
                try:
                    result['error'] = task.info.get('error')
                except Exception as e:
                    result['error'] = 'Unknown error occurred'

        response = jsonify(result)
        if state == states.PENDING:
            response.status_code = 202
        elif state == states.FAILURE:
            response.status_code = 500
        return response
