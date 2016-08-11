import time

from celery import states
from dive.task_core import celery, task_app

import logging
logger = logging.getLogger(__name__)

@celery.task(bind=True)
def error_handler(self, task_id):
    result = self.app.AsyncResult(task_id)
    self.update_state(
        task_id=task_id,
        state=states.FAILURE,
        meta={'error': result.traceback}
    )
    logger.error('Task {0} raised exception: {1!r}\n{2!r}'.format(
          task_id, result.result, result.traceback))
