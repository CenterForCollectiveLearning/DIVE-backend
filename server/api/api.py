import os
from os import listdir
from os.path import isfile, join
import re
import shutil
from random import sample
import pandas as pd
import xlrd

import cairocffi as cairo
import cairosvg
from StringIO import StringIO

import demjson
from flask import Flask, jsonify, request, make_response, json, send_file, session
from flask.json import JSONEncoder
from flask.ext.restful import Resource, Api, reqparse
from bson.objectid import ObjectId

from data.db import MongoInstance as MI
from data.access import upload_file, get_dataset_data, get_dataset_structure, get_column_types, get_delimiter, is_numeric
from analysis.analysis import compute_ontologies, get_ontologies
from properties import get_properties, get_entities, get_attributes, compute_properties
from visualization.viz_specs import get_viz_specs
from visualization.viz_data import getVisualizationDataFromSpec
from visualization.viz_stats import getVisualizationStats
from statistics.statistics import getStatisticsFromSpec

app = Flask(__name__)
app.debug = True
api = Api(app)

ALLOWED_EXTENSIONS = set(['txt', 'csv', 'tsv', 'xlsx', 'xls', 'json'])


# TODO: Make more general method for json formatting
class RoundedFloat(float):
    def __repr__(self):
        return '%.3f' % self

def format_json(obj):
    if isinstance(obj, float):
        return RoundedFloat(obj)
    elif isinstance(obj, dict):
        return dict((k, format_json(v)) for k, v in obj.items())
    elif isinstance(obj, (list, tuple)):
        return map(format_json, obj)
    return obj


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


# File upload handler
uploadFileParser = reqparse.RequestParser()
uploadFileParser.add_argument('pID', type=str, required=True)
class UploadFile(Resource):
    def post(self):
        form_data = json.loads(request.form.get('data'))
        pID = form_data.get('pID').strip().strip('"')
        file = request.files.get('file')

        if file and allowed_file(file.filename):
            # Get document with metadata and some samples
            datasets = upload_file(pID, file)
            json_data = {
                'status': 'success',
                'datasets': datasets
            }

            data = MI.getData({"$or" : map(lambda x: {"_id" : ObjectId(x['dID'])}, datasets)}, pID)
            properties_by_dID = compute_properties(pID, data)
            print "Done initializing properties"

            # compute_ontologies(pID, data)
            # print "Done initializing ontologies"

            return make_response(jsonify(format_json(json_data)))
        return make_response(jsonify(format_json({'status': 'Upload failed'})))


# Public Dataset retrieval
publicDataGetParser = reqparse.RequestParser()
publicDataGetParser.add_argument('dID', type=str, action='append')
publicDataGetParser.add_argument('sample', type=str, required=True, default='true')

# Use public dataset in project
publicDataPostParser = reqparse.RequestParser()
publicDataPostParser.add_argument('dID', type=str, action='append')
publicDataPostParser.add_argument('pID', type=str, required=True, default='true')
class Public_Data(Resource):
    # Get dataset descriptions or samples
    def get(self):
        args = publicDataGetParser.parse_args()
        dIDs = args.get('dID')
        pID = 'dive'
        print "[GET] PUBLIC Data", pID, dIDs

        # Specific dIDs
        if dIDs:
            print "Requested specific dIDs:", dIDs
            dataLocations = [ MI.getData({'_id': ObjectId(dID)}, pID) for dID in dIDs ]

        # All datasets
        else:
            print "Did not request specific dID. Returning all datasets"
            datasets = MI.getData({}, pID)
            data_list = []
            for d in datasets:
                path = d['path']
                result.update({
                    'title': d['title'],
                    'filename': d['filename'],
                    'dID': d['dID'],
                })
                data_list.append(result)
            return make_response(jsonify(format_json({'status': 'success', 'datasets': data_list})))

    def post(self):
        args = publicDataPostParser.parse_args()
        dIDs = args.get('dID')
        pID = args.get('pID')

        # Get data for selected datasets
        formatted_dIDs = [ObjectId(dID) for dID in dIDs]
        new_dIDs = MI.usePublicDataset({'_id': {'$in': formatted_dIDs}}, pID)
        datasets = MI.getData({'_id': {'$in': new_dIDs}}, pID)

        # Compute properties and ontologies
        compute_properties(pID, datasets)
        print "Done initializing properties"

        compute_ontologies(pID, datasets)
        print "Done initializing ontologies"

        data_list = []
        for d in datasets:
            # New dID
            # result = get_sample_data(d['path'])
            result.update({
                'title': d['title'],
                'filename': d['filename'],
                'path': d['path'],
                'dID': d['dID']
                })
            data_list.append(result)

        return make_response(jsonify(format_json({'status': 'success', 'datasets': data_list})))


# Datasets list retrieval
datasetsGetParser = reqparse.RequestParser()
datasetsGetParser.add_argument('pID', type=str, required=True)
datasetsGetParser.add_argument('getStructure', type=bool, required=False, default=False)
class Datasets(Resource):
    # Get dataset descriptions or samples
    def get(self):
        args = datasetsGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        print "[GET] Data", pID

        print "Did not request specific dID. Returning all datasets"
        datasets = MI.getData({}, pID)
        print datasets
        data_list = []
        for d in datasets:
            dataset_data = {
                'title': d['title'],
                'filename': d['filename'],
                'dID': d['dID']
            }

            if args['getStructure']:
                dataset_data['details'] = get_dataset_structure(d['path'])

            data_list.append(dataset_data)

        return make_response(jsonify(format_json({'status': 'success', 'datasets': data_list})))


# Dataset retrieval, editing, deletion
datasetGetParser = reqparse.RequestParser()
datasetGetParser.add_argument('pID', type=str, required=True)

datasetDeleteParser = reqparse.RequestParser()
datasetDeleteParser.add_argument('pID', type=str, action='append', required=True)
class Dataset(Resource):
    # Get dataset descriptions or samples
    def get(self, dID):
        args = datasetGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        print "[GET] Data", pID, dID

        print "Requested specific dID:", dID

        dataset = MI.getData({'_id': ObjectId(dID)}, pID)[0]

        response = {
            'dID': dataset['dID'],
            'title': dataset['title'],
            'details': get_dataset_data(dataset['path'])
        }

        return make_response(jsonify(format_json(response)))


    def delete(self, dID):
        args = datasetDeleteParser.parse_args()
        pID = args.get('pID')[0]

        # TODO Handle this formatting on the client side (or server side for additional safety?)
        pID = pID.strip().strip('"')
        dID = dID.strip().strip('"')
        return [ MI.deleteData(dID, pID) ]


############################
# Get Project ID from Title
############################
projectIDGetParser = reqparse.RequestParser()
projectIDGetParser.add_argument('formattedProjectTitle', type=str, required=True)
projectIDGetParser.add_argument('user_name', type=str, required=True)
class GetProjectID(Resource):
    def get(self):
        args = projectIDGetParser.parse_args()
        formattedProjectTitle = args.get('formattedProjectTitle')
        userName = args.get('user_name')
        print "GET projectID", formattedProjectTitle
        res = MI.getProjectID(formattedProjectTitle, userName)
        print "projectID result", res
        # TODO turn this into a proper response
        return res


############################
# Projects
############################
projectGetParser = reqparse.RequestParser()
projectGetParser.add_argument('pID', type=str, default='')
projectGetParser.add_argument('user_name', type=str, required=True)

projectPostParser = reqparse.RequestParser()
projectPostParser.add_argument('title', type=str, required=True)
projectPostParser.add_argument('description', type=str, required=False)
projectPostParser.add_argument('user_name', type=str, required=False)
projectPostParser.add_argument('anonymous', type=bool, required=False, default=False)

projectDeleteParser = reqparse.RequestParser()
projectDeleteParser.add_argument('pID', type=str, default='')
class Project(Resource):
    def get(self):
        args = projectGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        user_name = args.get('user_name')
        print "GET", pID, user_name
        return MI.getProject(pID, user_name)

    # Create project, initialize directories and collections
    def post(self):
        args = projectPostParser.parse_args()
        title = args.get('title')
        description = args.get('description')
        user_name = args.get('user_name')
        anonymous = args.get('anonymous')

        result = MI.postProject(title, description, user_name, anonymous)

        # If successful project creation
        if result[1] is 200:
            # Create data upload directory
            print "Created upload directory for pID:", result[0]['pID']
            os.mkdir(os.path.join(app.config['UPLOAD_FOLDER'], result[0]['pID']))

        return result

    # Delete project and all associated data
    def delete(self):
        args = projectDeleteParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        print "DELETE", pID

        MI.deleteProject(pID)

        # Delete uploads directory
        shutil.rmtree(os.path.join(app.config['UPLOAD_FOLDER'], pID))
        return


############################
# Property (begins processing on first client API call)
# Determine: types, hierarchies, uniqueness (subset of distributions), ontology, distributions
# INPUT: pID, dID
# OUTPUT: properties corresponding to that dID
############################
propertiesGetParser = reqparse.RequestParser()
propertiesGetParser.add_argument('pID', type=str, required=True)
propertiesGetParser.add_argument('dID', type=str, required=True)
class Properties(Resource):
    def get(self):
        print "[GET] Properties"
        args = propertiesGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        dID = args.get('dID')

        dataset_docs = MI.getData({"_id": ObjectId(dID)}, pID)

        # Parse properties into right return format (maybe don't do on this layer)
        properties = []

        results = {
            'properties': get_properties(pID, dataset_docs)
        }

        return make_response(jsonify(format_json(results)))


propertyGetParser = reqparse.RequestParser()
propertyGetParser.add_argument('pID', type=str, required=True)
class Property(Resource):
    def get(self, propertyID):
        print "[GET] Properties"
        args = propertyGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')

        _property = MI.getProperty({"_id": ObjectId(propertyID)}, pID)

        results = _property

        return make_response(json.dumps(format_json(results)))


entitiesGetParser = reqparse.RequestParser()
entitiesGetParser.add_argument('pID', type=str, required=True)
entitiesGetParser.add_argument('dID', type=str, required=True)
class Entities(Resource):
    def get(self):
        print "[GET] Entities"
        args = entitiesGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        dID = args.get('dID')

        dataset_docs = MI.getData({"_id": ObjectId(dID)}, pID)

        entities = get_entities(pID, dataset_docs)
        results = {
            'entities': entities
        }

        return make_response(jsonify(format_json(results)))

entitiesGetParser = reqparse.RequestParser()
entitiesGetParser.add_argument('pID', type=str, required=True)
entitiesGetParser.add_argument('dID', type=str, required=True)
class Attributes(Resource):
    def get(self):
        print "[GET] Entities"
        args = entitiesGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        dID = args.get('dID')

        dataset_docs = MI.getData({"_id": ObjectId(dID)}, pID)

        attributes = get_attributes(pID, dataset_docs)
        results = {
            'attributes': attributes
        }

        return make_response(jsonify(format_json(results)))


#####################################################################
# Endpoint returning all inferred visualization specifications for a specific project
# INPUT: pID, uID
# OUTPUT: {visualizationType: [visualizationSpecification]}
#####################################################################
vizSpecsGetParser = reqparse.RequestParser()
vizSpecsGetParser.add_argument('pID', type=str, required=True)
class Viz_Specs(Resource):
    def get(self):
        args = vizSpecsGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        specs_by_dID = get_viz_specs(pID)

        formatted_specs = {}
        for dID, specs in specs_by_dID.iteritems():
            specs_with_count = {
                'num_specs': len(specs),
                'specs': specs
            }
            formatted_specs[dID] = specs_with_count
        return make_response(jsonify(format_json(formatted_specs)))

#####################################################################
# Endpoint returning aggregated visualization data given a specification ID
# INPUT: sID, pID, uID
# OUTPUT: {nested visualization data}
#####################################################################

visualizationDataGetParser = reqparse.RequestParser()
visualizationDataGetParser.add_argument('pID', type=str, required=True)
visualizationDataGetParser.add_argument('spec', type=str, required=True)
visualizationDataGetParser.add_argument('conditional', type=str, required=True)
visualizationDataGetParser.add_argument('config', type=str, required=True)
class Visualization_Data(Resource):
    def get(self):
        args = visualizationDataGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        spec = json.loads(args.get('spec'))
        category = spec['category']
        conditional = json.loads(args.get('conditional'))
        config = json.loads(args.get('config'))

        resp = getVisualizationData(category, spec, conditional, config, pID)
        stats = getVisualizationStats(category, spec, conditional, config, pID)

        return make_response(jsonify(format_json({'result': resp, 'stats' : stats})))

#####################################################################
# Endpoint returning aggregated visualization data given a specification ID
# INPUT: sID, pID, uID
# OUTPUT: {nested visualization data}
#####################################################################

# For inferred visualizations
dataFromSpecPostParser = reqparse.RequestParser()
dataFromSpecPostParser.add_argument('dID', type=str, location='json')
dataFromSpecPostParser.add_argument('spec', type=str, location='json')
dataFromSpecPostParser.add_argument('conditional', type=str, location='json')

class Data_From_Spec(Resource):
    def post(self):
        args = request.json
        # TODO Implement required parameters
        pID = args.get('pID')
        spec = args.get('spec')
        conditional = args.get('conditional')

        result, status = getVisualizationDataFromSpec(spec, conditional, pID)
        return make_response(jsonify(format_json(result)), status)


#####################################################################
# Endpoint returning aggregated visualization data given a specification
# INPUT: pID, spec, conditionals
# OUTPUT: {nested visualization data, table data}
#####################################################################

# For inferred visualizations
dataFromSpecPostParser = reqparse.RequestParser()
dataFromSpecPostParser.add_argument('dID', type=str, location='json')
dataFromSpecPostParser.add_argument('spec', type=str, location='json')
dataFromSpecPostParser.add_argument('conditional', type=str, location='json')

class Data_From_Spec(Resource):
    def post(self):
        args = request.json
        # TODO Implement required parameters
        pID = args.get('pID')
        spec = args.get('spec')
        conditional = args.get('conditional')

        result, status = getVisualizationDataFromSpec(spec, conditional, pID)
        return make_response(jsonify(format_json(result)), status)


#####################################################################
# Endpoint returning statistical data given a specification
# INPUT: pID, spec
# OUTPUT: {stat data}
#####################################################################

# For inferred visualizations
statsFromSpecPostParser = reqparse.RequestParser()
statsFromSpecPostParser.add_argument('dID', type=str, location='json')
statsFromSpecPostParser.add_argument('spec', type=str, location='json')
class Statistics_From_Spec(Resource):
    def post(self):
        args = request.json
        # TODO Implement required parameters
        pID = args.get('pID')
        spec = args.get('spec')

        result, status = getStatisticsFromSpec(spec, pID)
        print result
        return make_response(jsonify(format_json(result)), status)


#####################################################################
# Endpoint returning data to populate dropdowns for given specification
# INPUT: sID, pID, uID
# OUTPUT: [conditional elements]
#####################################################################
chooseSpecParser = reqparse.RequestParser()
chooseSpecParser.add_argument('pID', type=str, required=True)
chooseSpecParser.add_argument('sID', type=str, required=True)
chooseSpecParser.add_argument('conditional', type=str, required=True)
class Choose_Spec(Resource):
    def get(self):
        args = chooseSpecParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        sID = args.get('sID')
        conditional = json.loads(args.get('conditional'))

        spec = MI.getSpecs(pID, {"_id" : ObjectId(sID)})[0]
        stats = getVisualizationStats(spec['category'], spec, conditional, pID)

        print "Choose spec", pID, sID, conditional, stats
        MI.chooseSpec(pID, sID, conditional, stats)
        return

rejectSpecParser = reqparse.RequestParser()
rejectSpecParser.add_argument('pID', type=str, required=True)
rejectSpecParser.add_argument('sID', type=str, required=True)
class Reject_Spec(Resource):
    def get(self):
        args = rejectSpecParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        sID = args.get('sID')
        MI.rejectSpec(pID, sID)
        return


#####################################################################
# Endpoint returning data to populate dropdowns for given specification
# INPUT: sID, pID, uID
# OUTPUT: [conditional elements]
#####################################################################
conditionalDataGetParser = reqparse.RequestParser()
conditionalDataGetParser.add_argument('pID', type=str, required=True)
conditionalDataGetParser.add_argument('dID', type=str, required=True)
conditionalDataGetParser.add_argument('spec', type=str, required=True)
class Conditional_Data(Resource):
    def get(self):
        args = conditionalDataGetParser.parse_args()
        print "GET COND DATA", args
        pID = args.get('pID').strip().strip('"')
        dID = args.get('dID').strip().strip('"')
        spec = json.loads(args.get('spec'))

        return make_response(jsonify(format_json({'result': getConditionalData(spec, dID, pID)})))


#####################################################################
# Endpoint returning exported viz specs given a pID and optionally matching an eID
#####################################################################
exportedVisualizationSpecGetParser = reqparse.RequestParser()
exportedVisualizationSpecGetParser.add_argument('pID', type=str, required=True)
exportedVisualizationSpecGetParser.add_argument('eID', type=str, required=False)
class Exported_Visualization_Spec(Resource):
    # Return all exported viz specs, grouped by category
    def get(self):
        args = exportedVisualizationSpecGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')

        find_doc = {}
        if args.get('eID'):
            eID = args.get('eID').strip().strip('"')
            find_doc = {'_id': ObjectId(eID)}
        exported_specs = MI.getExportedSpecs(find_doc, pID)

        specs_by_category = {}
        for exported_doc in exported_specs:
            spec = exported_doc['spec']
            category = spec['category']
            if category not in specs_by_category :
                specs_by_category[category] = []
            specs_by_category[category].append(exported_doc)

        return make_response(jsonify(format_json({'result': specs_by_category, 'length': len(exported_specs)})))

    def post(self):
        args = request.json['params']
        pID = args['pID']
        spec = args['spec']
        conditional = args['conditional']
        print "Posting exported visualization with args", args

        return make_response(jsonify(format_json({'result': MI.addExportedSpec(pID, spec, conditional)})))

#####################################################################
# Endpoint returning exported image
#####################################################################
renderedSVGPostParser = reqparse.RequestParser()
renderedSVGPostParser.add_argument('data')
class Render_SVG(Resource):
    def post(self):
        args = renderedSVGPostParser.parse_args()
        data = json.loads(args.get('data'))
        format = data['format']
        svg = data['svg']

        filename = 'visualization.%s' % format
        fout = open(filename, 'wb')
        print "Writing file"

        mimetypes = {
            'svg': 'image/svg',
            'pdf': 'application/pdf',
            'png': 'image/png'
        }

        img_io = StringIO()
        bytestring = bytes(svg)
        if format == "png":
            print "Rendering PNG"
            cairosvg.svg2png(bytestring=bytestring, write_to=fout)
            cairosvg.svg2png(bytestring=bytestring, write_to=img_io)
        elif format == "pdf":
            print "Rendering PDF"
            cairosvg.svg2pdf(bytestring=bytestring, write_to=fout)
            cairosvg.svg2pdf(bytestring=bytestring, write_to=img_io)
        elif format == "svg":
            print "Rendering SVG"
            cairosvg.svg2svg(bytestring=bytestring, write_to=fout)
            cairosvg.svg2svg(bytestring=bytestring, write_to=img_io)
        else:
            cairosvg.svg2png(bytestring=bytestring, write_to=fout)
            cairosvg.svg2png(bytestring=bytestring, write_to=img_io)
        fout.close()

        img_io.seek(0)
        return send_file(img_io)  #, mimetype=mimetypes[format], as_attachment=True, attachment_filename=filename)


#####################################################################
# TEST Endpoint
#####################################################################
class Test(Resource):
    def get(self):
        return make_response(jsonify(format_json({'result': 'test'})))


api.add_resource(Test,                          '/api/test')
api.add_resource(Public_Data,                   '/api/public_data')
api.add_resource(Render_SVG,                    '/api/render_svg')
api.add_resource(UploadFile,                    '/api/upload')
api.add_resource(Datasets,                      '/api/datasets')
api.add_resource(Dataset,                       '/api/datasets/<string:dID>')
api.add_resource(GetProjectID,                  '/api/getProjectID')
api.add_resource(Project,                       '/api/project')

api.add_resource(Properties,                    '/api/properties/v1/properties')
api.add_resource(Property,                      '/api/properties/v1/properties/<string:propertyID>')
api.add_resource(Entities,                      '/api/properties/v1/entities')
api.add_resource(Attributes,                    '/api/properties/v1/attributes')

api.add_resource(Viz_Specs,                     '/api/viz_specs')
api.add_resource(Visualization_Data,            '/api/visualization_data')
api.add_resource(Data_From_Spec,                '/api/data_from_spec')
api.add_resource(Statistics_From_Spec,          '/api/statistics_from_spec')
api.add_resource(Conditional_Data,              '/api/conditional_data')
api.add_resource(Exported_Visualization_Spec,   '/api/exported_spec')

from session import *
