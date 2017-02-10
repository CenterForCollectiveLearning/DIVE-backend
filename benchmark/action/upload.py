import os
from benchmark.action.base_action import Action


class Upload(Action):

    ACTION_ARG_WHITELIST = ['dive_url', 'file']

    def __init__(self, dive_url, file):
        self._dive_url = dive_url
        self._file = file
        super(Upload, self).__init__()

    def run(self, args):
        with open(self._file, 'rb') as file:
            files = {'data': ('', '{"project_id": 1}'), 'file': (os.path.basename(file.name), file)}
            response = args['session'].post('%s/datasets/v1/upload' % self._dive_url, files=files)
            return response