""" Base Benchmark Action API """


class Action(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def run(self, args):
        raise NotImplementedError('Implement this class')

    def is_valid(self):
        raise NotImplementedError('Implement this class')
