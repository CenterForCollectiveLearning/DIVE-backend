'''
Script to reset development environment (clean database and upload paths)
'''
import os
import shutil
from bson.objectid import ObjectId
from db import MongoInstance
from config import config


def remove_uploads():
    print "Removing data directories in upload folder"
    UPLOAD_FOLDER = os.path.join(os.curdir, config['UPLOAD_FOLDER'])
    shutil.rmtree(UPLOAD_FOLDER)


def create_directories():
    print "Creating upload and public data folder"
    UPLOAD_FOLDER = os.path.join(os.curdir, config['UPLOAD_FOLDER'])
    PUBLIC_DATA_FOLDER = os.path.join(os.curdir, config['PUBLIC_DATA_FOLDER'])
    os.mkdir(UPLOAD_FOLDER)
    if not os.path.exists(PUBLIC_DATA_FOLDER):
        os.mkdir(PUBLIC_DATA_FOLDER)


def clean_database():
    pIDs = [str(e['_id']) for e in MongoInstance.client['dive'].projects.find()]

    print "Removing project dbs"
    for pID in pIDs:
        MongoInstance.client.drop_database(pID)

    print "Cleaning projects from DIVE db"
    for pID in pIDs:
        MongoInstance.client['dive'].projects.remove({'_id': ObjectId(pID)})

    print "Cleaning users from DIVE db"
    MongoInstance.client['dive'].users.remove()

    print "Cleaning preloaded datasets from DIVE db"
    MongoInstance.client['dive'].datasets.remove()
    return


if __name__ == '__main__':
    print "Resetting production environment"
    remove_uploads()
    create_directories()
    clean_database()