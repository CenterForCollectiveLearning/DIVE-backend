import os
from dive.task_core import celery, task_app

@celery.task
def log_error(task_id):
    result = app.AsyncResult(task_id)
    result.get(propagate=False)  # make sure result written.
    with open(os.path.join('/var/errors', task_id), 'a') as fh:
        print('--\n\n{0} {1} {2}'.format(
            task_id, result.result, result.traceback), file=fh)
