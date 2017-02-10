from benchmark.action.base_action import Action


class Upload(Action):

    ACTION_ARG_WHITELIST = ['dive_url', 'file']

    def __init__(self, dive_url, file):
        self._dive_url = dive_url
        self._file = file
        super(Upload, self).__init__()

    def run(self, args):
        files = {'filename': open(self._file, 'rb')}
        args['session'].post('%s/datasets/v1/upload' % self._dive_url, files=files, data={"project_id": 1})
