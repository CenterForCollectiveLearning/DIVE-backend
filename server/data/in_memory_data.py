class inMemoryData(object):

    def __init__(self):
        self.data = {}

    def insertData(self, dID, df):
        self.data[dID] = df

    def hasData(self, dID):
        if dID in self.data:
            return True
        else:
            return False

    def getData(self, dID):
        return self.data[dID]

    def removeData(self, dID):
        del self.data[dID]

InMemoryData = inMemoryData()