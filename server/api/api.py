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

from flask import Flask, jsonify, request, make_response, json, send_file, session
from flask.ext.restful import Resource, Api, reqparse
from bson.objectid import ObjectId

from data.db import MongoInstance as MI
from data.access import upload_file, get_sample_data, get_column_types, get_delimiter, is_numeric
from analysis.analysis import detect_unique_list, compute_properties, compute_ontologies, get_properties, get_ontologies
from visualization.viz_specs import getVisualizationSpecs
from visualization.viz_data import getVisualizationData, getConditionalData
from visualization.viz_stats import getVisualizationStats
from config import config

app = Flask(__name__, static_path='/static')
app.config['SERVER_NAME'] = "localhost:8888"

api = Api(app)

TEST_DATA_FOLDER = os.path.join(os.curdir, config['TEST_DATA_FOLDER'])
app.config['TEST_DATA_FOLDER'] = TEST_DATA_FOLDER

PUBLIC_DATA_FOLDER = os.path.join(os.curdir, config['PUBLIC_DATA_FOLDER'])
app.config['PUBLIC_DATA_FOLDER'] = PUBLIC_DATA_FOLDER

UPLOAD_FOLDER = os.path.join(os.curdir, config['UPLOAD_FOLDER'])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


ALLOWED_EXTENSIONS = set(['txt', 'csv', 'tsv', 'xlsx', 'xls', 'json'])


@app.before_request
def option_autoreply():

    """ Always reply 200 on OPTIONS request """
    if request.method == 'OPTIONS':
        resp = app.make_default_options_response()

        print "Here comes an OPTIONS request"

        headers = None
        if 'ACCESS_CONTROL_REQUEST_HEADERS' in request.headers:
            headers = request.headers['ACCESS_CONTROL_REQUEST_HEADERS']

        h = resp.headers

        # Allow the origin which made the XHR
        h['Access-Control-Allow-Origin'] = request.headers['Origin']
        # Allow the actual method
        h['Access-Control-Allow-Methods'] = request.headers['Access-Control-Request-Method']
        # Allow for 10 seconds
        h['Access-Control-Max-Age'] = "10"

        # We also keep current headers
        if headers is not None:
            h['Access-Control-Allow-Headers'] = headers

        return resp

@app.after_request
def replace_nan(resp):
    cleaned_data = resp.get_data().replace('nan', 'null').replace('NaN', 'null')
    resp.set_data(cleaned_data)
    return resp

@app.after_request
def set_allow_origin(resp):
    """ Set origin for GET, POST, PUT, DELETE requests """

    h = resp.headers

    # Allow crossdomain for other HTTP Verbs
    if request.method != 'OPTIONS' and 'Origin' in request.headers:
        h['Access-Control-Allow-Origin'] = request.headers['Origin']
    return resp


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


# File upload handler
uploadFileParser = reqparse.RequestParser()
uploadFileParser.add_argument('pID', type=str, required=True)
class UploadFile(Resource):
    def post(self):
        pID = request.form.get('pID').strip().strip('"')
        file = request.files.get('file')

        if file and allowed_file(file.filename):
            # Get document with metadata and some samples
            datasets = upload_file(pID, file)
            json_data = {
                'status': 'success',
                'datasets': datasets
            }

            data = MI.getData({"$or" : map(lambda x: {"_id" : ObjectId(x['dID'])}, datasets)}, pID)
            compute_properties(pID, data)
            print "Done initializing properties"

            # compute_ontologies(pID, data)
            # print "Done initializing ontologies"

            response = make_response(jsonify(json_data))
            return response
        return make_response(jsonify({'status': 'Upload failed'}))


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
                result = get_sample_data(path)
                result.update({
                    'title': d['title'],
                    'filename': d['filename'],
                    'dID': d['dID'],
                })
                data_list.append(result)
            return make_response(jsonify({'status': 'success', 'datasets': data_list}))

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
            result = get_sample_data(d['path'])
            result.update({
                'title': d['title'],
                'filename': d['filename'],
                'path': d['path'],
                'dID': d['dID']
                })
            data_list.append(result)

        return make_response(jsonify({'status': 'success', 'datasets': data_list}))


# Dataset retrieval, editing, deletion
dataGetParser = reqparse.RequestParser()
dataGetParser.add_argument('dID', type=str, action='append')
dataGetParser.add_argument('pID', type=str, required=True)
dataGetParser.add_argument('sample', type=str, required=True, default='true')

dataDeleteParser = reqparse.RequestParser()
dataDeleteParser.add_argument('dID', type=str, action='append', required=True)
dataDeleteParser.add_argument('pID', type=str, action='append', required=True)
class Data(Resource):
    # Get dataset descriptions or samples
    def get(self):
        args = dataGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        dIDs = args.get('dID')
        print "[GET] Data", pID, dIDs

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

                result = get_sample_data(path)
                result.update({
                    'title': d['title'],
                    'filename': d['filename'],
                    'dID': d['dID']
                })
                data_list.append(result)
            return make_response(jsonify({'status': 'success', 'datasets': data_list}))

    def delete(self):
        args = dataDeleteParser.parse_args()
        pIDs = args.get('pID')
        dIDs = args.get('dID')

        # TODO Handle this formatting on the client side (or server side for additional safety?)
        pIDs = [ pID.strip().strip('"') for pID in pIDs ]
        dIDs = [ dID.strip().strip('"') for dID in dIDs ]
        params = zip(dIDs, pIDs)
        deleted_dIDs = [ MI.deleteData(dID, pID) for (dID, pID) in params ]
        return deleted_dIDs


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
        return res


############################
# Projects
############################
projectGetParser = reqparse.RequestParser()
projectGetParser.add_argument('pID', type=str, default='')
projectGetParser.add_argument('user_name', type=str, required=True)

projectPostParser = reqparse.RequestParser()
projectPostParser.add_argument('title', type=str, required=True)
projectPostParser.add_argument('description', type=str, required=True)
projectPostParser.add_argument('user_name', type=str, required=True)

projectDeleteParser = reqparse.RequestParser()
projectDeleteParser.add_argument('pID', type=str, default='')
class Project(Resource):
    def get(self):
        print "hello!"
        args = projectGetParser.parse_args()
        print args
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

        result = MI.postProject(title, description, user_name)

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
# INPUT: pID
# OUTPUT: {types: types_dict, uniques: is_unique_dict, overlaps: overlaps, hierarchies: hierarchies}
############################
propertyGetParser = reqparse.RequestParser()
propertyGetParser.add_argument('pID', type=str, required=True)
# propertyPutParser = reqparse.RequestParser()
# propertyPutParser.add_argument('ontologies', type=)
class Property(Resource):
    def get(self):
        print "[GET] Properties"
        args = propertyGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        datasets = MI.getData({}, pID)
        
        print "Getting properties"
        stats, types, headers, is_unique = get_properties(pID, datasets)

        print "Getting ontologies"
        overlaps, hierarchies = get_ontologies(pID, datasets)

        all_properties = {
            'types': types, 
            'uniques': is_unique,
            'stats': stats,
            'overlaps': overlaps, 
            'hierarchies': hierarchies,
        }

        return make_response(jsonify(all_properties))

## approach:
## each data upload -> add in new ontologies
    def put(self) :
        print "[PUT] Properties"
        pID = request.json['params']['pID']
        ontologies = request.json['params']['ontologies']
        # print "Updating to ", len(ontologies.keys()), " links"

        MI.resetOntology(pID)

        for link in ontologies.keys() :
            [dID, dID2, col, col2] = link.split(",")
            d, h = ontologies[link]

            o = {
                'source_dID' : dID,
                'target_dID' : dID2,
                'source_index' : col,
                'target_index' : col2,
                'distance' : d,
                'hierarchy' : h
            }
            MI.upsertOntology(pID, o)
        return make_response(jsonify({}))

#####################################################################
# Endpoint returning all inferred visualization specifications for a specific project
# INPUT: pID, uID
# OUTPUT: {visualizationType: [visualizationSpecification]}
#####################################################################
specificationGetParser = reqparse.RequestParser()
specificationGetParser.add_argument('pID', type=str, required=True)
specificationGetParser.add_argument('sID', type=str, action='append')
class Specification(Resource):
    def get(self):
        args = specificationGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        specs_by_viz_type = getVisualizationSpecs(pID)
        return make_response(jsonify(specs_by_viz_type))


#####################################################################
# Endpoint returning aggregated visualization data given a specification ID
# INPUT: sID, pID, uID
# OUTPUT: {nested visualization data}
#####################################################################
visualizationDataGetParser = reqparse.RequestParser()
visualizationDataGetParser.add_argument('pID', type=str, required=True)
visualizationDataGetParser.add_argument('spec', type=str, required=True)
visualizationDataGetParser.add_argument('conditional', type=str, required=True)
class Visualization_Data(Resource):
    def get(self):
        args = visualizationDataGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        spec = json.loads(args.get('spec'))
        viz_type = spec['viz_type']
        conditional = json.loads(args.get('conditional'))

        resp = getVisualizationData(viz_type, spec, conditional, pID)

        stats = {}
        if (len(resp) > 0) :
            stats = getVisualizationStats(viz_type, spec, conditional, pID)

        return make_response(jsonify({'result': resp, 'stats' : stats}))


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
        stats = getVisualizationStats(spec['viz_type'], spec, conditional, pID)

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
        
        return make_response(jsonify({'result': getConditionalData(spec, dID, pID)}))


#####################################################################
# Endpoint returning exported viz specs given a pID and optionally matching an eID
#####################################################################
exportedVisualizationSpecGetParser = reqparse.RequestParser()
exportedVisualizationSpecGetParser.add_argument('pID', type=str, required=True)
exportedVisualizationSpecGetParser.add_argument('eID', type=str, required=False)
class Exported_Visualization_Spec(Resource):
    def get(self):
        args = exportedVisualizationSpecGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')

        find_doc = {}
        if args.get('eID'):
            eID = args.get('eID').strip().strip('"')
            find_doc = {'_id': ObjectId(eID)}
        return make_response(jsonify({'result': MI.getExportedSpecs(find_doc, pID)}))


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

        filename = 'test.%s' % format
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

        img_io.seek(10)
        return send_file(img_io)  #, mimetype=mimetypes[format], as_attachment=True, attachment_filename=filename)

api.add_resource(Public_Data, '/api/public_data')
api.add_resource(Render_SVG, '/api/render_svg')
api.add_resource(UploadFile, '/api/upload')
api.add_resource(Data, '/api/data')
api.add_resource(GetProjectID, '/api/getProjectID')
api.add_resource(Project, '/api/project')
api.add_resource(Property, '/api/property')
api.add_resource(Specification, '/api/specification')
api.add_resource(Choose_Spec, '/api/choose_spec')
api.add_resource(Reject_Spec, '/api/reject_spec')
api.add_resource(Visualization_Data, '/api/visualization_data')
api.add_resource(Conditional_Data, '/api/conditional_data')
api.add_resource(Exported_Visualization_Spec, '/api/exported_spec')

from session import *