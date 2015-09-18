'''
Script to add preloaded datasets to DIVE database
'''
from os import listdir, curdir
from os.path import isfile, join
from bson.objectid import ObjectId
from dive.db import MongoInstance as MI
from config import config


def preload():
    PUBLIC_FOLDER = join(curdir, config['PUBLIC_DATA_FOLDER'])

    # For each directory
    for d in listdir(PUBLIC_FOLDER):
        if (not d[0].startswith('.')):
            print "DIRECTORY:", d
            # For each file in directory
            for f in listdir(join(PUBLIC_FOLDER, d)):
                print '\tFILE:', f
                MI.insertDataset('dive', join(PUBLIC_FOLDER, d, f), f)


if __name__ == '__main__':
    print "Preloading datasets"
    preload()