'''
Our data access layer
'''
from utility import *

class DAL(object):

    def __init__(self):
        self.dataset_ids = {}
        self.column_ids = {}
        self.num_datasets = 0
        return

    def insert_dataset(self, path):
        if path not in self.dataset_ids:
            self.dataset_ids[path] = self.num_datasets
            self.column_ids[path] = self.get_column_ids(path)
            self.num_datasets += 1
        return

    def get_path_from_id(self, passed_id):
        for path, stored_id in self.dataset_ids.iteritems():
            if stored_id == passed_id:
                return path
        return

    def get_column_name_from_id(self, passed_dataset_id, passed_column_id):
        for path, stored_id in self.column_ids.iteritems():
            if stored_id == passed_id:
                return path
        return

    def get_dataset_id(self, path):
        return self.dataset_ids[path]

    def get_column_ids(self, path):
        return self.column_ids[path]

    # Return list of dataset and column unique IDs
    # Currently datasets and attributes are just numbers (UUIDs are overkill)
    @staticmethod
    def get_column_ids(path):
        # TODO Abstract this file reading
        f = open(path)
        delim = get_delimiter(path)
        l = f.readline().split(delim)

        return [i for i in range(0, len(l))]

# A singleton object
DAL = DAL()