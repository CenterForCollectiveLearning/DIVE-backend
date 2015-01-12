'''
Script to reset development environment (clean database and upload paths)
'''
import os
import shutil
from bson.objectid import ObjectId
from db import MongoInstance



def remove_uploads():
    print "1) Removing data directories in upload folder"
    UPLOAD_FOLDER = os.path.join(os.curdir, 'uploads')
    shutil.rmtree(UPLOAD_FOLDER)
    os.mkdir(UPLOAD_FOLDER)


def clean_database():
    pIDs = [str(e['_id']) for e in MongoInstance.client['dive'].projects.find()]

    print "2) Removing project dbs"
    for pID in pIDs:
        MongoInstance.client.drop_database(pID)

    print "3) Cleaning DIVE db"
    for pID in pIDs:
        MongoInstance.client['dive'].projects.remove({'_id': ObjectId(pID)})
    return


if __name__ == '__main__':
    print "Resetting production environment"
    remove_uploads()
    clean_database()