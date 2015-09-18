from flask.ext.restful import Api
from dive.resources.datasets import UploadFile, Dataset, Datasets, PreloadedDatasets
from dive.resources.projects import Project, Projects
from dive.resources.field_properties import FieldProperties
from dive.resources.specs import Specs, VisualizationFromSpec, Visualization, GeneratingProcedures
from dive.resources.statistics_resources import StatisticsFromSpec, RegressionEstimator
from dive.resources.exported_specs import ExportedSpecs, VisualizationFromExportedSpec
from dive.resources.render import Render
# from dive.resources.auth import Register, Login

from flask.ext.restful import Resource

class Test(Resource):
    def get(self):
        return 'Success'

api = Api()

api.add_resource(Test, '/test')

api.add_resource(Projects,                      '/projects/v1/projects')
api.add_resource(Project,                       '/projects/v1/projects/<project_id>')

api.add_resource(UploadFile,                    '/datasets/v1/upload')
api.add_resource(Datasets,                      '/datasets/v1/datasets')
api.add_resource(PreloadedDatasets,             '/datasets/v1/datasets/preloaded')
api.add_resource(Dataset,                       '/datasets/v1/datasets/<string:dataset_id>')

api.add_resource(FieldProperties,               '/field_properties/v1/field_properties')

api.add_resource(Specs,                         '/specs/v1/specs')
api.add_resource(VisualizationFromSpec,         '/specs/v1/specs/<string:spec_id>/visualization')
api.add_resource(GeneratingProcedures,          '/specs/v1/generating_procedures')

api.add_resource(ExportedSpecs,                 '/exported_specs/v1/exported_specs')
api.add_resource(VisualizationFromExportedSpec, '/exported_specs/v1/exported_specs/<string:exported_spec_id>/visualization')

api.add_resource(Render,                        '/render/v1/render')

api.add_resource(StatisticsFromSpec,            '/statistics/v1/statistics_from_spec')
api.add_resource(RegressionEstimator,           '/statistics/v1/regression_estimator')

# api.add_resource(Register,                      '/auth/v1/register')
# api.add_resource(Login,                         '/auth/v1/login')
