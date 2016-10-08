from dive.server.resources.datasets import UploadFile, Dataset, Datasets
from dive.server.resources.documents import NewDocument, Document, Documents
from dive.server.resources.fields import Field
from dive.server.resources.projects import Project, Projects
from dive.server.resources.field_properties import FieldProperties
from dive.server.resources.specs import Specs, VisualizationFromSpec, GeneratingProcedures

from dive.server.resources.statistics_resources import AnovaFromSpec, CorrelationsFromSpec, RegressionEstimator, \
    RegressionFromSpec, AggregationStatsFromSpec, NumericalComparisonFromSpec, \
    OneDimensionalTableFromSpec, ContingencyTableFromSpec, InteractionTerms, \
    ContributionToRSquared, CorrelationScatterplot, AnovaBoxplotFromSpec, PairwiseComparisonFromSpec

from dive.server.resources.exported_results import ExportedResults
from dive.server.resources.exported_specs import ExportedSpecs, VisualizationFromExportedSpec
from dive.server.resources.exported_analyses import ExportedRegression, DataFromExportedRegression, \
    ExportedCorrelation, DataFromExportedCorrelation, ExportedAggregation, DataFromExportedAggregation

from dive.server.resources.transform import Reduce, Unpivot, Join

from dive.server.resources.task_resources import TaskResult, RevokeTask, RevokeChainTask
from dive.server.resources.auth_resources import Register, Login, Logout, User

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

    api.add_resource(InteractionTerms,              '/statistics/v1/interaction_term')

    api.add_resource(RegressionFromSpec,            '/statistics/v1/regression')
    api.add_resource(ContributionToRSquared,        '/statistics/v1/contribution_to_r_squared')
    api.add_resource(AggregationStatsFromSpec,          '/statistics/v1/aggregation_stats')
    api.add_resource(OneDimensionalTableFromSpec,   '/statistics/v1/one_dimensional_contingency_table')

    api.add_resource(AnovaFromSpec,                 '/statistics/v1/anova')
    api.add_resource(AnovaBoxplotFromSpec,          '/statistics/v1/anova_boxplot')
    api.add_resource(PairwiseComparisonFromSpec,    '/statistics/v1/pairwise_comparison')
    api.add_resource(ContingencyTableFromSpec,      '/statistics/v1/contingency_table')
    api.add_resource(NumericalComparisonFromSpec,   '/statistics/v1/numerical_comparison')
    api.add_resource(CorrelationsFromSpec,          '/statistics/v1/correlations')
    api.add_resource(CorrelationScatterplot,        '/statistics/v1/correlation_scatterplot')
    api.add_resource(RegressionEstimator,           '/statistics/v1/regression_estimator')

    api.add_resource(ExportedResults,               '/exported_results/v1/exported_results')

    api.add_resource(ExportedRegression,            '/exported_regression/v1/exported_regression')
    api.add_resource(DataFromExportedRegression,    '/exported_regression/v1/exported_regression/<string:exported_spec_id>/data')

    api.add_resource(ExportedCorrelation,           '/exported_correlation/v1/exported_correlation')
    api.add_resource(DataFromExportedCorrelation,   '/exported_correlation/v1/exported_correlation/<string:exported_spec_id>/data')

    api.add_resource(ExportedAggregation,           '/exported_aggregation/v1/exported_aggregation')
    api.add_resource(DataFromExportedAggregation,   '/exported_aggregation/v1/exported_aggregation/<string:exported_spec_id>/data')

    api.add_resource(Documents,                     '/compose/v1/documents')
    api.add_resource(NewDocument,                   '/compose/v1/document')
    api.add_resource(Document,                      '/compose/v1/document/<string:document_id>')

    api.add_resource(Register,                      '/auth/v1/register')
    api.add_resource(Login,                         '/auth/v1/login')
    api.add_resource(Logout,                        '/auth/v1/logout')
    api.add_resource(User,                          '/auth/v1/user')

    api.add_resource(Feedback,                      '/feedback/v1/feedback')

    return api
