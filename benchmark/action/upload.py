import os
import logging
import time
from benchmark.action.base_action import Action

LOG = logging.getLogger(__name__)


class Upload(Action):

    UPLOAD_TASK_PENDING = 'PENDING'

    ACTION_ARG_WHITELIST = ['dive_url', 'file', 'delay', 'threshold_millis']

    def __init__(self, dive_url, file, threshold_millis=0, delay=0):
        self._dive_url = dive_url
        self._file = file
        self._delay = delay
        self._threshold_millis = threshold_millis
        super(Upload, self).__init__()

    def run(self, args):
        LOG.info('Starting upload action')
        LOG.info('Delaying {0} seconds'.format(str(self._delay)))
        time.sleep(self._delay)
        LOG.info('Finished delay')
        with open(self._file, 'rb') as file:
            project_response = args['session'].post('%s/projects/v1/projects' % self._dive_url, json={
                'anonymous': 'false',
                'description': 'Project Description',
                'title': 'Project Title',
                'user_id': args['user_id']
            })
            files = {
                'data': ('', '{"project_id": %s}' % project_response.json()["id"]),
                'file': (os.path.basename(file.name), file)
            }
            LOG.info("Starting upload for: %s" % file.name)
            start_time = time.time()
            response = args['session'].post('%s/datasets/v1/upload' % self._dive_url, files=files)
            task_id = response.json()['taskId']
            task_status = self.UPLOAD_TASK_PENDING
            while task_status == self.UPLOAD_TASK_PENDING:
                time.sleep(0.5)
                if (time.time() - start_time) * 1000 > self._threshold_millis:
                    error = "Elapsed time exceeded threshold millis %s" % str(self._threshold_millis)
                    LOG.error(error)
                    raise AssertionError(error)
                status_response = args['session'].get('{0}/tasks/v1/result/{1}'.format(self._dive_url, task_id))
                task_status = status_response.json()['state']
            elapsed = time.time() - start_time
            LOG.info("Upload took %s seconds" % str(elapsed))
            return response