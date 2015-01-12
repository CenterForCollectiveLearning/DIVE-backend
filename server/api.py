import os
from os import listdir
from os.path import isfile, join
import re
import shutil
from random import sample
import pandas as pd

from flask import Flask, render_template, redirect, url_for, request, make_response, json
from flask.ext.restful import Resource, Api, reqparse
from bson.objectid import ObjectId
from werkzeug.utils import secure_filename

from db import MongoInstance as MI
from data import get_sample_data, read_file, get_column_types, get_delimiter, is_numeric
from analysis import detect_unique_list, compute_properties, compute_ontologies
from specifications import *
from visualization_data import getVisualizationData, getConditionalData
from utility import *


PORT = 8888

app = Flask(__name__, static_path='/static')
api = Api(app)

TEST_DATA_FOLDER = os.path.join(os.curdir, 'test_data')
app.config['TEST_DATA_FOLDER'] = TEST_DATA_FOLDER

UPLOAD_FOLDER = os.path.join(os.curdir, 'uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

ALLOWED_EXTENSIONS = set(['txt', 'csv', 'tsv', 'xlsx', 'xls', 'json'])


@app.before_request
def option_autoreply():
    """ Always reply 200 on OPTIONS request """

    if request.method == 'OPTIONS':
        resp = app.make_default_options_response()

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
    # Dataflow: 
    # 1. Save file in uploads/pID directory
    # 2. Save file location in project data collection
    # 3. Return sample
    def post(self):
        pID = request.form.get('pID').strip().strip('"')
        file = request.files.get('file')

        if file and allowed_file(file.filename):
            # Save file
            filename = secure_filename(file.filename)
            print "Saving file: ", filename
            path = os.path.join(app.config['UPLOAD_FOLDER'], pID, filename)
            file.save(path)

            # Insert into project's datasets collection
            dID = MI.insertDataset(pID, path, file)

            # Get sample data
            sample, rows, cols, extension, header = get_sample_data(path)
            types = get_column_types(path)
            header, columns = read_file(path)
            column_attrs = [{'name': header[i], 'type': types[i], 'column_id': i} for i in range(0, len(columns) - 1)]

            # Make response
            json_data = json.jsonify({
                'status': 'success',
                'title': filename.split('.')[0],
                'filename': filename,
                'dID': dID,
                'column_attrs': column_attrs,
                'filename': filename,
                'header': header,
                'sample': sample,
                'rows': rows,
                'cols': cols,
                'filetype': extension,
            })
            response = make_response(json_data)
            response.set_cookie('file', filename)
            return response
        return json.jsonify({'status': 'Upload failed'})


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
                path = os.path.join(app.config['UPLOAD_FOLDER'], pID, d['filename'])

                types = get_column_types(path)

                header, columns = read_file(path)
                unique_cols = [detect_unique_list(col) for col in columns]
    
                # make response
                sample, rows, cols, extension, header = get_sample_data(path)
                types = get_column_types(path)
                column_attrs = [{'name': header[i], 'type': types[i], 'column_id': i} for i in range(0, len(columns) - 1)]

                # Make response
                json_data = {
                    'title': d['filename'].split('.')[0],
                    'filename': d['filename'],
                    'dID': d['dID'],
                    'column_attrs': column_attrs,
                    'header': header,
                    'sample': sample,
                    'rows': rows,
                    'cols': cols,
                    'filetype': extension,
                }
                data_list.append(json_data)
            return json.jsonify({'status': 'success', 'datasets': data_list})

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
class GetProjectID(Resource):
    def get(self):
        args = projectIDGetParser.parse_args()
        formattedProjectTitle = args.get('formattedProjectTitle')
        print "GET projectID", formattedProjectTitle
        res = MI.getProjectID(formattedProjectTitle)
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
class Property(Resource):
    def get(self):
        print "[GET] Properties"
        args = propertyGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        datasets = MI.getData({}, pID)
        
        # Compute properties of all datasets
        stats, types, headers, is_unique = compute_properties(pID, datasets)

        # Compute cross-dataset overlaps        
        overlaps, hierarchies = compute_ontologies(pID, datasets)

        all_properties = {
            'types': types, 
            'uniques': is_unique,
            'stats': stats,
            'overlaps': overlaps, 
            'hierarchies': hierarchies,
        }

        return json.jsonify(all_properties)

#####################################################################
# Endpoint returning all inferred visualization specifications for a specific project
# INPUT: pID, uID
# OUTPUT: {visualizationType: [visualizationSpecification]}
#####################################################################
specificationDataGetParser = reqparse.RequestParser()
specificationDataGetParser.add_argument('pID', type=str, required=True)
specificationDataGetParser.add_argument('sID', type=str, action='append')
class Specification(Resource):
    def get(self):
        args = specificationDataGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')

        d = MI.getData(None, pID)
        p = MI.getProperty(None, pID)
        o = MI.getOntology(None, pID)

        viz_types = {
            "treemap": getTreemapSpecs(d, p, o),
            "piechart": getPiechartSpecs(d, p, o),
            "geomap": getGeomapSpecs(d, p, o),
            # "barchart": getBarchartSpecs(d, p, o),
            "scatterplot": getScatterplotSpecs(d, p, o),
            "linechart": getLinechartSpecs(d, p, o),
            # "network": getNetworkSpecs(d, p, o)
        }

        for viz_type, specs in viz_types.iteritems():
            if specs:
                sIDs = MI.postSpecs(pID, specs) 
                for i, spec in enumerate(specs):
                    spec['sID'] = sIDs[i]
                    del spec['_id']
        return viz_types


#####################################################################
# Endpoint returning aggregated visualization data given a specification ID
# INPUT: sID, pID, uID
# OUTPUT: {nested visualization data}
#####################################################################
visualizationDataGetParser = reqparse.RequestParser()
visualizationDataGetParser.add_argument('pID', type=str, required=True)
visualizationDataGetParser.add_argument('type', type=str, required=True)
visualizationDataGetParser.add_argument('spec', type=str, required=True)
visualizationDataGetParser.add_argument('conditional', type=str, required=True)
class Visualization_Data(Resource):
    def get(self):
        print "Getting viz data"
        args = visualizationDataGetParser.parse_args()
        pID = args.get('pID').strip().strip('"')
        type = args.get('type')
        spec = json.loads(args.get('spec'))
        conditional = json.loads(args.get('conditional'))

        return json.jsonify(getVisualizationData(type, spec, conditional, pID))


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

        print "Choose spec", pID, sID, conditional
        MI.chooseSpec(pID, sID, conditional)
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
        pID = args.get('pID').strip().strip('"')
        dID = args.get('dID').strip().strip('"')
        spec = json.loads(args.get('spec'))
        return json.jsonify({'result': getConditionalData(spec, dID, pID)})


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
        return json.jsonify({'result': MI.getExportedSpecs(find_doc, pID)})


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


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=PORT)
