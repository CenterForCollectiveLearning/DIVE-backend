from flask import make_response, jsonify, current_app, url_for
from flask.ext.restful import Resource, reqparse, marshal_with

from celery import states, chain, group
from celery.result import result_from_tuple, ResultSet

from dive.task_core import celery
from dive.resources.utilities import format_json
from dive.tasks.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline

import logging
logger = logging.getLogger(__name__)


def object_type(j):
    return j


class RevokeTask(Resource):
    def get(self, task_id):
        logger.debug('Revoking task: %s', task_id)
        r = celery.control.revoke(task_id,
            terminate = True,
            signal = 'SIGKILL'
        )
        logger.debug(r)


revokeChainTaskPostParser = reqparse.RequestParser()
revokeChainTaskPostParser.add_argument('task_ids', type=object_type, required=True, location='json')
class RevokeChainTask(Resource):
    def post(self):
        args = revokeChainTaskPostParser.parse_args()
        task_ids = args.get('task_ids')
        logger.debug('Revoking tasks: %s', task_ids)
        celery.control.revoke(task_ids,
            terminate = True,
            signal = 'SIGKILL'
        )


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
                    'current_task': task.info.get('desc'),
                    'state': task.state,
                }
            else:
                state = {
                    'current_task': '',
                    'state': task.state,
                }
        elif task.state == states.SUCCESS:
            state = {
                'result': task.info.get('result'),
                'state': task.state,
            }
        response = jsonify(format_json(state))
        if task.state == states.PENDING:
            response.status_code = 202
        return response


chainTaskResultPostParser = reqparse.RequestParser()
chainTaskResultPostParser.add_argument('task_ids', type=object_type, required=True, location='json')
class ChainTaskResult(Resource):
    '''
    If not all tasks are completed, return description for current task and previous task.
    If all tasks are completed, return result
    '''
    def post(self):
        args = chainTaskResultPostParser.parse_args()
        task_ids = args.get('task_ids')

        num_tasks = len(task_ids)
        all_success = True
        previous_task = ''
        current_task = ''
        most_recent_result = None
        for i, task_id in enumerate(task_ids):
            step = i + 1
            task = celery.AsyncResult(task_id)

            if task.state == states.SUCCESS:
                previous_task = '(%s/%s) %s' % (step, num_tasks, task.info.get('desc'))
                most_recent_result = task.info.get('result')

            if task.state == states.PENDING:
                all_success = False
                if (task.info) and (task.info.get('desc')):
                    current_task = '(%s/%s) %s' % (step, num_tasks, task.info.get('desc'))


        if all_success:
            result = {
                'state': states.SUCCESS,
                'result': most_recent_result
            }
        else:
            result = {
                'state': states.PENDING,
                'previous_task': previous_task,
                'current_task': current_task
            }
        return jsonify(format_json(result))
