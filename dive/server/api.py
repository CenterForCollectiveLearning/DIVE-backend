from dive.server.resources.datasets import UploadFile, Dataset, Datasets, PreloadedDatasets, SelectPreloadedDataset, DeselectPreloadedDataset
from dive.server.resources.documents import NewDocument, Document, Documents
from dive.server.resources.fields import Field
from dive.server.resources.projects import Project, Projects
from dive.server.resources.field_properties_resources import FieldProperties
from dive.server.resources.specs import Specs, VisualizationFromSpec, GeneratingProcedures

from dive.server.resources.statistics_resources import ComparisonFromSpec, CorrelationsFromSpec, RegressionEstimator, \
    RegressionFromSpec, AggregationFromSpec, InteractionTerms, \
    InitialRegressionModelRecommendation

from dive.server.resources.exported_specs import ExportedSpecs, VisualizationFromExportedSpec
from dive.server.resources.exported_analyses import ExportedAnalyses, ExportedRegression, DataFromExportedRegression, \
    ExportedCorrelation, DataFromExportedCorrelation, ExportedAggregation, DataFromExportedAggregation, \
    ExportedComparison, DataFromExportedComparison

from dive.server.resources.transform import Reduce, Unpivot, Join
from dive.server.resources.task_resources import TaskResult, RevokeTask, RevokeChainTask
from dive.server.resources.auth_resources import Register, Login, Logout, User, Confirm_Token, Resend_Email, Reset_Password_Link, Reset_Password_With_Token, AnonymousUser, DeleteAnonymousData

from flask import request, make_response
from dive.server.resources.feedback import Feedback
from dive.base.serialization import jsonify

from flask_restful import Resource

class Test(Resource):
    def get(self):
        return make_response(jsonify({ 'result': 'Success' }))


def add_resources(api):
    api.add_resource(Test,                          '/test')

    api.add_resource(TaskResult,                    '/tasks/v1/result/<task_id>')
    api.add_resource(RevokeTask,                    '/tasks/v1/revoke/<task_id>')
    api.add_resource(RevokeChainTask,               '/tasks/v1/revoke')

    api.add_resource(Projects,                      '/projects/v1/projects')
    api.add_resource(Project,                       '/projects/v1/projects/<project_id>')

    api.add_resource(UploadFile,                    '/datasets/v1/upload')
    api.add_resource(Datasets,                      '/datasets/v1/datasets')
    api.add_resource(Dataset,                       '/datasets/v1/datasets/<int:dataset_id>')
    api.add_resource(PreloadedDatasets,             '/datasets/v1/preloaded_datasets')
    api.add_resource(SelectPreloadedDataset,        '/datasets/v1/select_preloaded_dataset')
    api.add_resource(DeselectPreloadedDataset,      '/datasets/v1/deselect_preloaded_dataset')

    api.add_resource(Reduce,                        '/datasets/v1/reduce')
    api.add_resource(Unpivot,                       '/datasets/v1/unpivot')
    api.add_resource(Join,                          '/datasets/v1/join')

    api.add_resource(Field,                         '/datasets/v1/fields/<int:field_id>')

    api.add_resource(FieldProperties,               '/field_properties/v1/field_properties')

    api.add_resource(Specs,                         '/specs/v1/specs')
    api.add_resource(VisualizationFromSpec,         '/specs/v1/specs/<int:spec_id>/visualization')
    api.add_resource(GeneratingProcedures,          '/specs/v1/generating_procedures')

    api.add_resource(ExportedSpecs,                 '/exported_specs/v1/exported_specs')
    api.add_resource(VisualizationFromExportedSpec, '/exported_specs/v1/exported_specs/<int:exported_spec_id>/visualization')

    api.add_resource(InteractionTerms,              '/statistics/v1/interaction_term')

    api.add_resource(RegressionFromSpec,            '/statistics/v1/regression')
    api.add_resource(AggregationFromSpec,           '/statistics/v1/aggregation')

    api.add_resource(ComparisonFromSpec,            '/statistics/v1/comparison')
    api.add_resource(CorrelationsFromSpec,          '/statistics/v1/correlations')
    api.add_resource(RegressionEstimator,           '/statistics/v1/regression_estimator')
    api.add_resource(InitialRegressionModelRecommendation, '/statistics/v1/initial_regression_state')

    api.add_resource(ExportedAnalyses,               '/exported_analyses/v1/exported_analyses')

    api.add_resource(ExportedRegression,            '/exported_regression/v1/exported_regression')
    api.add_resource(DataFromExportedRegression,    '/exported_regression/v1/exported_regression/<int:exported_spec_id>/data')

    api.add_resource(ExportedCorrelation,           '/exported_correlation/v1/exported_correlation')
    api.add_resource(DataFromExportedCorrelation,   '/exported_correlation/v1/exported_correlation/<int:exported_spec_id>/data')

    api.add_resource(ExportedAggregation,           '/exported_aggregation/v1/exported_aggregation')
    api.add_resource(DataFromExportedAggregation,   '/exported_aggregation/v1/exported_aggregation/<int:exported_spec_id>/data')

    api.add_resource(ExportedComparison,            '/exported_comparison/v1/exported_comparison')
    api.add_resource(DataFromExportedComparison,    '/exported_comparison/v1/exported_comparison/<int:exported_spec_id>/data')

    api.add_resource(Documents,                     '/compose/v1/documents')
    api.add_resource(NewDocument,                   '/compose/v1/document')
    api.add_resource(Document,                      '/compose/v1/document/<int:document_id>')

    api.add_resource(Confirm_Token,                 '/auth/v1/confirm/<string:token>')
    api.add_resource(Register,                      '/auth/v1/register')
    api.add_resource(Login,                         '/auth/v1/login')
    api.add_resource(Logout,                        '/auth/v1/logout')
    api.add_resource(User,                          '/auth/v1/user')
    api.add_resource(Resend_Email,                  '/auth/v1/resend')
    api.add_resource(Reset_Password_Link,           '/auth/v1/reset_password')
    api.add_resource(Reset_Password_With_Token,     '/auth/v1/reset_password/<string:token>')
    api.add_resource(AnonymousUser,                 '/auth/v1/anonymous_user')
    api.add_resource(DeleteAnonymousData,           '/auth/v1/delete_anonymous_data/<int:user_id>')

    api.add_resource(Feedback,                      '/feedback/v1/feedback')

    return api
