from flask import make_response, jsonify, current_app, url_for
from flask_restful import Resource, reqparse, marshal_with

from celery import states
from celery.result import result_from_tuple, ResultSet, AsyncResult

from dive.worker.core import celery, task_app
from dive.base.serialization import jsonify
from dive.worker.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline

import logging
logger = logging.getLogger(__name__)


class RevokeTask(Resource):
    def get(self, task_id):
        logger.debug('Revoking task: %s', task_id)
        r = celery.control.revoke(task_id)

revokeChainTaskPostParser = reqparse.RequestParser()
revokeChainTaskPostParser.add_argument('task_ids', type=list, required=True, location='json')
class RevokeChainTask(Resource):
    def post(self):
        args = revokeChainTaskPostParser.parse_args()
        task_ids = args.get('task_ids')
        logger.debug('Revoking tasks: %s', task_ids)

        # TODO Terminate or not?
        r = celery.control.revoke(task_ids, terminate=False)

task_state_to_code = {
    states.SUCCESS: 200,
    states.PENDING: 202,
    states.FAILURE: 500,
    states.REVOKED: 500
}

class TaskResult(Resource):
    def get(self, task_id):
        task = celery.AsyncResult(task_id)  # task_2 = AsyncResult(id=task_id, app=celery)
        state = task.state
        info = task.info if task.info else {}
        result = {
            'state': state
        }

        if (state == states.PENDING):
            try:
                result['currentTask'] = info.get('desc', 'Processing Data')
            except AttributeError:
                state = states.FAILURE
                result['state'] = states.FAILURE
                result['currentTask'] = task.info

        elif (state == states.SUCCESS):
            result['result'] = info.get('result', None)

        elif (state == states.FAILURE):
            if info:
                error_type = type(info).__name__
                error_message = '%s: %s' % (error_type, str(info))
            else:
                error_type = None
                error_message = 'Unknown error occurred'
            result['error'] = error_message

        response = jsonify(result, status=task_state_to_code[state])
        return response
