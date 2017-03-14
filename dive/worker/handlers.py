import time

from celery import states, current_task
from dive.worker.core import celery, task_app

import logging
logger = logging.getLogger(__name__)


@celery.task()
def worker_error_handler(request, exc, traceback):
    task_id = request.id
    task = celery.AsyncResult(task_id)
    current_task.update_state(
        task_id=task_id,
        state=states.FAILURE,
        meta={ 'error': traceback }
    )
    logger.error('Task {0} raised exception: {1!r}\n{2!r}'.format(task_id, exc, traceback))
    # REPORT with raven
    # return traceback
