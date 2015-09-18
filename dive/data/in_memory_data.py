class inMemoryData(object):

    def __init__(self):
        self.data = {}

    def insertData(self, dataset_id, df):
        self.data[dataset_id] = df

    def hasData(self, dataset_id):
        if dataset_id in self.data:
            return True
        else:
            return False

    def getData(self, dataset_id):
        return self.data[dataset_id]

    def removeData(self, dataset_id):
        del self.data[dataset_id]

InMemoryData = inMemoryData()