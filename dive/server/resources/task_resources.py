from flask import make_response, jsonify, current_app, url_for
from flask_restful import Resource, reqparse, marshal_with

from celery import states
from celery.result import result_from_tuple, ResultSet, AsyncResult

from dive.worker.core import celery, task_app
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
    def get(self, task_id):
        task_result = AsyncResult(id=task_id, app=celery)
        result = {
            'currentTask': '',
            'state': task_result.state
        }
        state = task_result.state
        print 'Task:', task_id, task_result, task_result.state, task_result.get()

        if state == states.FAILURE or (task_result.state == 'SUCCESS' and task_result.get() == 'FAILURE'):
            result['state'] = states.FAILURE
            if task_result.info:
                result['error'] = task_result.info.get('error', '')
            else:
                result['error'] = 'An error has occurred'
                
        elif state == states.PENDING:
            try:
                if (task_result.info) and (task_result.info.get('desc')):
                    result['currentTask'] = task_result.info.get('desc')
                else:
                    result['currentTask'] = 'Loading'
            except AttributeError:
                if (task_result.info):
                    state = states.FAILURE
                    result['state'] = states.FAILURE
                    result['currentTask'] = task_result.info
            logger.info('Pending task %s: %s', task_id, state)

        elif state == states.SUCCESS:
            if task_result.info:
                result['result'] = task_result.info.get('result')

        status = 200
        if state == states.PENDING:
            status = 202
        elif state == states.FAILURE:
            status = 500

        return jsonify(result, status=status)
