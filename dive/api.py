from dive.resources.datasets import UploadFile, Dataset, Datasets
from dive.resources.documents import NewDocument, Document
from dive.resources.fields import Field
from dive.resources.projects import Project, Projects
from dive.resources.field_properties import FieldProperties
from dive.resources.specs import Specs, VisualizationFromSpec, GeneratingProcedures
from dive.resources.exported_specs import ExportedSpecs, VisualizationFromExportedSpec
from dive.resources.statistics_resources import AnovaFromSpec, RegressionEstimator, RegressionFromSpec, SummaryStatsFromSpec, NumericalComparisonFromSpec, OneDimensionalTableFromSpec, ContingencyTableFromSpec, ComparisonFromSpec, SegmentationFromSpec, ContributionToRSquared
from dive.resources.exported_regressions import ExportedRegressions, DataFromExportedRegression
from dive.resources.transform import Reduce, Unpivot, Join

from dive.resources.task_resources import TaskResult, RevokeTask, RevokeChainTask
# from dive.resources.auth import Register, Login

from flask.ext.restful import Resource


def add_resources(api):
    api.add_resource(TaskResult,                    '/tasks/v1/result/<task_id>')
    api.add_resource(RevokeTask,                    '/tasks/v1/revoke/<task_id>')
    api.add_resource(RevokeChainTask,               '/tasks/v1/revoke')

    api.add_resource(Projects,                      '/projects/v1/projects')
    api.add_resource(Project,                       '/projects/v1/projects/<project_id>')

    api.add_resource(UploadFile,                    '/datasets/v1/upload')
    api.add_resource(Datasets,                      '/datasets/v1/datasets')
    # api.add_resource(PreloadedDatasets,           '/datasets/v1/datasets/preloaded')
    api.add_resource(Dataset,                       '/datasets/v1/datasets/<string:dataset_id>')

    api.add_resource(Reduce,                        '/datasets/v1/reduce')
    api.add_resource(Unpivot,                       '/datasets/v1/unpivot')
    api.add_resource(Join,                          '/datasets/v1/join')

    api.add_resource(Field,                         '/datasets/v1/fields/<string:field_id>')

    api.add_resource(FieldProperties,               '/field_properties/v1/field_properties')

    api.add_resource(Specs,                         '/specs/v1/specs')
    api.add_resource(VisualizationFromSpec,         '/specs/v1/specs/<string:spec_id>/visualization')
    api.add_resource(GeneratingProcedures,          '/specs/v1/generating_procedures')

    api.add_resource(ExportedSpecs,                 '/exported_specs/v1/exported_specs')
    api.add_resource(VisualizationFromExportedSpec, '/exported_specs/v1/exported_specs/<string:exported_spec_id>/visualization')

    api.add_resource(RegressionFromSpec,            '/statistics/v1/regression')
    api.add_resource(ContributionToRSquared,        '/statistics/v1/contribution_to_r_squared/<string:regression_id>')
    api.add_resource(ComparisonFromSpec,            '/statistics/v1/comparison')
    api.add_resource(SummaryStatsFromSpec,          '/statistics/v1/summary_stats')
    api.add_resource(NumericalComparisonFromSpec,   '/statistics/v1/numerical_comparison')
    api.add_resource(ContingencyTableFromSpec,      '/statistics/v1/contingency_table')
    api.add_resource(OneDimensionalTableFromSpec,   '/statistics/v1/one_dimensional_contingency_table')
    api.add_resource(AnovaFromSpec,                 '/statistics/v1/anova')
    api.add_resource(SegmentationFromSpec,          '/statistics/v1/segmentation')
    api.add_resource(RegressionEstimator,           '/statistics/v1/regression_estimator')

    api.add_resource(ExportedRegressions,           '/exported_regressions/v1/exported_regressions')
    api.add_resource(DataFromExportedRegression,    '/exported_regressions/v1/exported_regressions/<string:exported_spec_id>/data')

    api.add_resource(NewDocument,                   '/compose/v1/document')
    api.add_resource(Document,                      '/compose/v1/document/<string:document_id>')


    return api
    # api.add_resource(Register,                      '/auth/v1/register')
    # api.add_resource(Login,                         '/auth/v1/login')
