from flask import make_response, jsonify, current_app, url_for
from flask.ext.restful import Resource, reqparse, marshal_with

from celery import states
from celery.result import result_from_tuple

from dive.task_core import celery
from dive.resources.utilities import format_json
from dive.tasks.pipelines import ingestion_pipeline, viz_spec_pipeline, full_pipeline

import logging
logger = logging.getLogger(__name__)


def getChainIDs(task):
    parent = task.parent
    if parent:
        return getChainIDs(parent) + [ task.id ]  # Return first task first
    else:
        return [ task.id ]


class TestPipeline(Resource):
    def get(self, project_id, dataset_id):
        task = full_pipeline(dataset_id, project_id).apply_async()

        task_id = task.id
        logger.info(task_id)
        logger.info(getChainIDs(task))
        response = jsonify({'task_ids': getChainIDs(task)})
        response.status_code = 202
        return response


class TaskResult(Resource):
    def get(self, task_id):
        task = celery.AsyncResult(task_id)

        logger.info("STATE %s", task.state)
        if task.state == states.PENDING:
            state = {
                'state': task.state,
                'info': task.info
            }
        elif task.state != states.FAILURE:
            state = {
                'state': task.state,
                'info': task.info,
            }
        else:
            state = {
                'state': task.state,
                'status': str(task.info),
            }
        response = jsonify(state)
        response.status_code = 202
        return response
