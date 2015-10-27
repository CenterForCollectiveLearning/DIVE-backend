from dive.resources.datasets import UploadFile, Dataset, Datasets
from dive.resources.projects import Project, Projects
from dive.resources.field_properties import FieldProperties
from dive.resources.specs import Specs, VisualizationFromSpec, GeneratingProcedures
from dive.resources.exported_specs import ExportedSpecs, VisualizationFromExportedSpec
from dive.resources.statistics_resources import RegressionEstimator, RegressionFromSpec, ComparisonFromSpec, SegmentationFromSpec, ContributionToRSquared
from dive.resources.exported_regressions import ExportedRegressions, DataFromExportedRegression

from dive.resources.task_resources import TestPipeline, TaskResult
# from dive.resources.auth import Register, Login

from flask.ext.restful import Resource


def add_resources(api):
    api.add_resource(TestPipeline,                  '/test/<project_id>/<dataset_id>')
    api.add_resource(TaskResult,                    '/task_result/<task_id>')

    api.add_resource(Projects,                      '/projects/v1/projects')
    api.add_resource(Project,                       '/projects/v1/projects/<project_id>')

    api.add_resource(UploadFile,                    '/datasets/v1/upload')
    api.add_resource(Datasets,                      '/datasets/v1/datasets')
    # api.add_resource(PreloadedDatasets,           '/datasets/v1/datasets/preloaded')
    api.add_resource(Dataset,                       '/datasets/v1/datasets/<string:dataset_id>')

    api.add_resource(FieldProperties,               '/field_properties/v1/field_properties')

    api.add_resource(Specs,                         '/specs/v1/specs')
    api.add_resource(VisualizationFromSpec,         '/specs/v1/specs/<string:spec_id>/visualization')
    api.add_resource(GeneratingProcedures,          '/specs/v1/generating_procedures')

    api.add_resource(ExportedSpecs,                 '/exported_specs/v1/exported_specs')
    api.add_resource(VisualizationFromExportedSpec, '/exported_specs/v1/exported_specs/<string:exported_spec_id>/visualization')

    api.add_resource(RegressionFromSpec,            '/statistics/v1/regression')
    api.add_resource(ContributionToRSquared,        '/statistics/v1/contribution_to_r_squared/<string:regression_id>')
    api.add_resource(ComparisonFromSpec,            '/statistics/v1/comparison')
    api.add_resource(SegmentationFromSpec,          '/statistics/v1/segmentation')
    api.add_resource(RegressionEstimator,           '/statistics/v1/regression_estimator')

    api.add_resource(ExportedRegressions,           '/exported_regressions/v1/exported_regressions')
    api.add_resource(DataFromExportedRegression,    '/exported_regressions/v1/exported_regressions/<string:exported_spec_id>/data')


    return api
    # api.add_resource(Register,                      '/auth/v1/register')
    # api.add_resource(Login,                         '/auth/v1/login')
