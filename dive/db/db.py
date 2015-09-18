
import math
import pymongo
from pymongo import GEO2D
from bson.objectid import ObjectId
import os
import urllib
import random
import time
import json

from sqlalchemy import create_engine


# TODO: Use a SON manipulator?
def remove_dots(data):
    for k, d in data.iteritems():
        if type(d) is dict: d = remove_dots(d)
        if '.' in k and isinstance(k, basestring):
            data[k.replace('.', '\uff0E')] = data[k]
            del data[k]
    return data


def formatObjectIDs(collectionName, results, fullname = False):
    if fullname:
        propertyName = collectionName
    else:
        propertyName = collectionName[0]

    for result in results: # For each result is passed, convert the _id to the proper mID, cID, etc.
        result[propertyName + 'ID'] = str(result.pop('_id')) # Note the .pop removes the _id from the dict
    return results

def stringifyID(results):
    for result in results: # For each result is passed, convert the _id to the proper mID, cID, etc.
        result['id'] = str(result.pop('_id')) # Note the .pop removes the _id from the dict
    return results


class mongoInstance(object):
    # Get Project ID from formattedProjectTitle
    def getProjectID(self, formatted_title, userName = None):
        find_doc = {
            "formattedTitle" : formatted_title,
        }

        if userName:
            find_doc["user"] = userName

        try:
            return str(MongoInstance.client['dive'].projects.find_one(find_doc)['_id'])
        except TypeError:
            return None

    # Dataset Insertion
    def insertDataset(self, project_id, path, filename):

        title = filename.rsplit('.', 1)[0]
        file_type = filename.rsplit('.', 1)[1]

        dataset_doc = {
            'path': path,
            'filename': filename,
            'title': title,
            'type': file_type
        }

        return str(MongoInstance.client[project_id].datasets.insert(dataset_doc))

    # Dataset Retrieval
    def getData(self, find_doc, project_id):
        return formatObjectIDs('dataset', [ d for d in MongoInstance.client[project_id].datasets.find(find_doc) ])

    # Dataset Deletion
    def deleteData(self, dataset_id, project_id):
        MongoInstance.client[project_id].fieldProperties.remove({'dataset_id': dataset_id})
        resp = MongoInstance.client[project_id].datasets.remove({'_id': ObjectId(dataset_id)})
        if resp['n'] and resp['ok']:
            return dataset_id

    def setSpecs(self, specs, project_id):
        # TODO Don't drop specs every time
        MongoInstance.client[project_id].specifications.remove({})
        resp = MongoInstance.client[project_id].specifications.insert(specs)
        return [ str(sID_obj) for sID_obj in resp ]


    def getSpecs(self, find_doc, project_id):
        return stringifyID([s for s in MongoInstance.client[project_id].specifications.find(find_doc) ])

    # Using preloaded datasets
    def usePublicDataset(self, find_doc, project_id):
        publicDatasets = MongoInstance.client['dive'].datasets.find(find_doc)
        new_dataset_ids = []
        for d in publicDatasets:
            d['original_dataset_id'] = str(d['_id'])
            del d['_id']
            new_dataset_id = MongoInstance.client[project_id].datasets.insert(d)
            new_dataset_ids.append(new_dataset_id)
        return new_dataset_ids

    ################
    # Exported Specs (pointer to spec + conditional)
    ################
    def getExportedSpecs(self, find_doc, project_id):
        exported_specs = [ e for e in MongoInstance.client[project_id].exported.find(find_doc)]
        for spec in exported_specs:
            sID = spec['sID']
            spec_find_doc = { '_id': ObjectId(sID) }
            corresponding_spec = [ c for c in MongoInstance.client[project_id].specifications.find(spec_find_doc)]
            if corresponding_spec:
                spec['spec'] = corresponding_spec
            else:
                raise ValueError('sID %s does not correspond to a real spec' % (sID))
                continue

        return stringifyID(exported_specs)

    def insertExportedSpecs(self, sID, conditional, config, project_id):
        d = {}
        d['sID'] = sID
        d['conditional'] = conditional
        d['config'] = config
        return str(MongoInstance.client[project_id].exported.insert(d))

    def updateExportedSpecs(self, sID, conditional, config, project_id):
        d = {}
        d['sID'] = sID
        d['conditional'] = conditional
        d['config'] = config
        result = MongoInstance.client[project_id].exported.find_and_modify({'sID': sID},
            {'$set': {'conditional': conditional, 'config': config}},
            upsert=True, new=True)
        return str(result)

    def deleteExportedSpecs(self, find_doc, project_id):
        return str(MongoInstance.client[project_id].exported.remove(find_doc))

    ################
    # Project Editing
    ################
    def getProject(self, project_id, user):
        projects_collection = MongoInstance.client['dive'].projects
        doc = {
            'user': user
        }
        # if project_id: doc['_id'] = ObjectId(project_id)
        return formatObjectIDs('project', [ p for p in projects_collection.find(doc)])

    def deleteProject(self, project_id):
        # Drop top-level DB
        MongoInstance.client.drop_database(project_id)

        # Drop DB document in DIVE DB
        MongoInstance.client['dive'].projects.remove({'_id': ObjectId(project_id)})
        return

    ################
    # Field Properties
    ################
    def upsertFieldProperty(self, properties, dataset_id, project_id):
        info = MongoInstance.client[project_id].fieldProperties.find_and_modify({'dataset_id': dataset_id}, {'$set': properties}, upsert=True, new=True)
        tID = str(info['_id'])
        return tID

    def getFieldProperty(self, find_doc, project_id):
        return formatObjectIDs('property', [ t for t in MongoInstance.client[project_id].fieldProperties.find(find_doc) ], True)

    def setFieldProperty(self, _property, project_id):
        print "Saving field property"
        return MongoInstance.client[project_id].fieldProperties.insert(_property)

    ################
    # Dataset Properties
    ################
    def getDatasetProperty(self, find_doc, project_id):
        return formatObjectIDs('property', [ t for t in MongoInstance.client[project_id].datasetProperties.find(find_doc) ], True)

    def setDatasetProperty(self, _property, project_id):
        return MongoInstance.client[project_id].datasetProperties.insert(_property)

    def getOntology(self, find_doc, project_id):
        return formatObjectIDs('ontology', [ o for o in MongoInstance.client[project_id].ontologies.find(find_doc) ])

    def upsertOntology(self, project_id, ontology):
        o = ontology
        find_doc = {
            'source_dataset_id': o['source_dataset_id'],
            'target_dataset_id': o['target_dataset_id'],
            'source_index': o['source_index'],
            'target_index': o['target_index']
        }
        info = MongoInstance.client[project_id].ontologies.find_and_modify(find_doc, {'$set': ontology}, upsert=True, new=True)
        if info:
            oID = str(info['_id'])
            return oID
        else:
            print o

    def resetOntology(self, project_id) :
        return MongoInstance.client[project_id].ontologies.remove({})


    # Project Creation
    def postProject(self, title, description, user, anonymous):
        formatted_title = title.replace(" ", "-").lower()

        projects_collection = MongoInstance.client['dive'].projects
        existing_project = projects_collection.find_one({'formattedTitle': formatted_title, 'user': user})
        if existing_project:
            print "Project Already Exists"
            return str(existing_project['_id']), 409
        else:
            # Insert project into DIVE project collections
            project_id = str(projects_collection.insert({
                'formattedTitle': formatted_title,
                'title': title,
                'description': description,
                'user': user,
                'anonymous': anonymous
            }))

            # Create user
            # TODO Tie into projects
            # MongoInstance.client['dive'].users.insert({'userName': user})

            # Create project DB
            db = MongoInstance.client[project_id]
            db.create_collection('datasets')
            db.create_collection('visualizations')
            db.create_collection('datasetProperties')
            db.create_collection('fieldProperties')
            db.create_collection('ontologies')
            db.create_collection('exported')
            print "Creating new project"
            return {'formatted_title': formatted_title, 'project_id': project_id}, 200

    # User Creation
    def postNewUser(self, userName, displayName, password) :
        user = {
            'userName' : userName,
            'displayName' : displayName,
            'password' : password
        }
        # str(MongoInstance.client[project_id].datasets.insert(dataset_doc))
        return str(MongoInstance.client['dive'].users.insert(user))

    def getUser(self, find_doc) :
        return formatObjectIDs('users', [u for u in MongoInstance.client['dive'].users.find(find_doc).limit(1) ])
                # return formatObjectIDs('ontology', [ o for o in MongoInstance.client[project_id].ontologies.find(find_doc) ])

    # Client corresponding to a single connection
    @property
    def client(self):
        if not hasattr(self, '_client'):
            self._client = pymongo.MongoClient(host='localhost:27017')
        return self._client

# A Singleton Object
MongoInstance = mongoInstance()
MongoInstance.client['dive'].projects.ensure_index([("formattedTitle", True), ("user", True)])

engine = create_engine('postgresql://localhost:5432')
