import os
import sys
import logging
from flask import Flask, request
from flask.ext.restful import Api
from flask.ext.sqlalchemy import SQLAlchemy
from werkzeug.local import LocalProxy

app = Flask(__name__)
app.config.from_object('config.DevelopmentConfig')

api = Api(app)
db = SQLAlchemy(app)
logger = app.logger
app_config = app.config

UPLOAD_FOLDER = os.path.join(os.curdir, app.config['UPLOAD_FOLDER'])

from resources.datasets import UploadFile, Dataset, Datasets, PreloadedDatasets
from resources.projects import Projects
from resources.field_properties import FieldProperties
from resources.specs import Specs, VisualizationFromSpec, Visualization, GeneratingProcedures
from resources.statistics_resources import StatisticsFromSpec, RegressionEstimator
from resources.exported_specs import ExportedSpecs, VisualizationFromExportedSpec
from resources.render import Render
# from resources.auth import Register, Login

from flask.ext.restful import Resource

class Test(Resource):
    def get(self):
        return 'Succss'

api.add_resource(Test, '/test')

# Multiple projects per user
api.add_resource(Projects,                      '/projects/v1/projects')

# What do you get back here?
api.add_resource(UploadFile,                    '/datasets/v1/upload')
api.add_resource(Datasets,                      '/datasets/v1/datasets')  # Returns [ {properties}, {}], not including preloaded
api.add_resource(PreloadedDatasets,             '/datasets/v1/datasets/preloaded')  # Defer this
api.add_resource(Dataset,                       '/datasets/v1/datasets/<string:dID>')  # Returns preview data

api.add_resource(FieldProperties,               '/field_properties/v1/field_properties')

api.add_resource(Specs,                         '/specs/v1/specs')
api.add_resource(VisualizationFromSpec,         '/specs/v1/specs/<sID>/visualization')
api.add_resource(GeneratingProcedures,          '/specs/v1/generating_procedures')

api.add_resource(ExportedSpecs,                 '/exported_specs/v1/exported_specs')  # Get vs post
api.add_resource(VisualizationFromExportedSpec, '/exported_specs/v1/exported_specs/<eID>/visualization')

api.add_resource(Render,                        '/render/v1/render')

api.add_resource(StatisticsFromSpec,            '/statistics/v1/statistics_from_spec')
api.add_resource(RegressionEstimator,           '/statistics/v1/regression_estimator')

# api.add_resource(Register,                      '/auth/v1/register')
# api.add_resource(Login,                         '/auth/v1/login')

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
    print resp
    try:
        cleaned_data = resp.get_data().replace('nan', 'null').replace('NaN', 'null')
        resp.set_data(cleaned_data)
        return resp
    except:
        return resp


@app.after_request
def set_allow_origin(resp):
    """ Set origin for GET, POST, PUT, DELETE requests """

    h = resp.headers

    # Allow crossdomain for other HTTP Verbs
    if request.method != 'OPTIONS' and 'Origin' in request.headers:
        h['Access-Control-Allow-Origin'] = request.headers['Origin']
    return resp
