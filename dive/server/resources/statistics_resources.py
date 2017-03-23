import time
from flask import current_app, request, make_response
from flask_restful import Resource, reqparse

from dive.base.db import db_access
from dive.base.serialization import jsonify


# Sync tasks
from dive.base.constants import ModelRecommendationType as MRT, ModelCompletionType as MCT
from dive.worker.statistics.regression.rsquared import get_contribution_to_r_squared_data
from dive.worker.statistics.regression.model_recommendation import get_initial_regression_model_recommendation

# Async tasks
from dive.worker.pipelines import regression_pipeline, aggregation_pipeline, correlation_pipeline, one_dimensional_contingency_table_pipeline, contingency_table_pipeline, comparison_pipeline
from dive.worker.handlers import worker_error_handler

import logging
logger = logging.getLogger(__name__)

#####################################################################
# Endpoint returning estimated time for regression
# INPUT: numInputs, sizeArray, funcArraySize
# OUTPUT: time
#####################################################################
# For inferred visualizations
timeFromParamsPostParser = reqparse.RequestParser()
timeFromParamsPostParser.add_argument('numInputs', type=int, location='json')
timeFromParamsPostParser.add_argument('sizeArray', type=int, location='json')
timeFromParamsPostParser.add_argument('funcArraySize', type=int, location='json')
class RegressionEstimator(Resource):
    def post(self):
        args = timeFromParamsPostParser.parse_args()
        # TODO Implement required parameters
        numInputs = args.get('numInputs')
        sizeArray = args.get('sizeArray')
        funcArraySize = args.get('funcArraySize')

        result, status = timeEstimator(numInputs, sizeArray, funcArraySize)
        return make_response(jsonify(result))


contributionToRSquaredPostParser = reqparse.RequestParser()
contributionToRSquaredPostParser.add_argument('projectId', type=int, location='json')
contributionToRSquaredPostParser.add_argument('regressionId', type=int, location='json')
class ContributionToRSquared(Resource):
    def post(self):
        args = contributionToRSquaredPostParser.parse_args()
        project_id = args.get('projectId')
        regression_id = args.get('regressionId')
        regression_doc = db_access.get_regression_by_id(regression_id, project_id)
        regression_data = regression_doc['data']
        data = get_contribution_to_r_squared_data(regression_data)
        return jsonify({ 'data': data })

# For interaction term creation
interactionTermPostParser = reqparse.RequestParser()
interactionTermPostParser.add_argument('interactionTermIds', type=list, location='json')
interactionTermPostParser.add_argument('projectId', type=int, location='json')
interactionTermPostParser.add_argument('datasetId', type=int, location='json')

interactionTermDeleteParser = reqparse.RequestParser()
interactionTermDeleteParser.add_argument('id', type=str, required=True)
class InteractionTerms(Resource):
    def post(self):
        args = interactionTermPostParser.parse_args()
        project_id = args.get('projectId')
        dataset_id = args.get('datasetId')
        interaction_term_ids = args.get('interactionTermIds')
        data = db_access.insert_interaction_term(project_id, dataset_id, interaction_term_ids)
        return jsonify(data)

    def delete(self):
        args = interactionTermDeleteParser.parse_args()
        interaction_term_id = args.get('id')
        deleted_term = db_access.delete_interaction_term(interaction_term_id)
        return jsonify(deleted_term)

#####################################################################
# Return variables included in model selection algorithm
# INPUT: project_id, dataset_id
# OUTPUT: {}
#####################################################################
initialRegressionModelRecommendationPostParser = reqparse.RequestParser()
initialRegressionModelRecommendationPostParser.add_argument('projectId', required=True, type=int, location='json')
initialRegressionModelRecommendationPostParser.add_argument('datasetId', required=True, type=int, location='json')
initialRegressionModelRecommendationPostParser.add_argument('dependentVariableId', type=int, location='json')
initialRegressionModelRecommendationPostParser.add_argument('recommendationType', type=str, default=MRT.LASSO.value, location='json')
initialRegressionModelRecommendationPostParser.add_argument('tableLayout', type=str, default=MCT.LEAVE_ONE_OUT.value, location='json')
class InitialRegressionModelRecommendation(Resource):
    def post(self):
        args = initialRegressionModelRecommendationPostParser.parse_args()
        project_id = args.get('projectId')
        dataset_id = args.get('datasetId')
        dependent_variable_id = args.get('dependentVariableId')
        recommendation_type = args.get('recommendationType')
        table_layout = args.get('tableLayout')

        result = get_initial_regression_model_recommendation(project_id, dataset_id, dependent_variable_id=dependent_variable_id, recommendation_type=recommendation_type, table_layout=table_layout)
        return jsonify(result)


#####################################################################
# Endpoint returning regression data given a specification
# INPUT: project_id, spec
# OUTPUT: {stat data}
#####################################################################
regressionPostParser = reqparse.RequestParser()
regressionPostParser.add_argument('projectId', type=int, location='json')
regressionPostParser.add_argument('spec', type=dict, location='json')
regressionPostParser.add_argument('conditionals', type=dict, location='json', default={})
class RegressionFromSpec(Resource):
    def post(self):
        '''
        spec: {
            independentVariables
            dependentVariable
            interactionTerms
            model
            estimator
            degree
            weights
            functions
            datasetId
            tableLayout
        }
        '''

        args = regressionPostParser.parse_args()
        project_id = args.get('projectId')
        spec = args.get('spec')
        conditionals = args.get('conditionals', {})

        regression_doc = db_access.get_regression_from_spec(project_id, spec, conditionals=conditionals)

        # check to see if regression is in db; if so, send back data
        if regression_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            regression_data = regression_doc['data']
            regression_data['id'] = regression_doc['id']

            exported_regression_doc = db_access.get_exported_regression_by_regression_id(project_id, regression_doc['id'])
            if exported_regression_doc:
                regression_data['exported'] = True
                regression_data['exportedRegressionId'] = exported_regression_doc['id']
            else:
                regression_data['exported'] = False
            return jsonify(regression_data)
        else:
            regression_task = regression_pipeline.apply_async(
                args = [ spec, project_id, conditionals ]
            )

            return jsonify({
                'task_id': regression_task.task_id,
                'compute': True
            }, status=202)


#####################################################################
# Endpoint returning comparison data given a specification
# INPUT: project_id, spec
# OUTPUT: {stat data}
#####################################################################
comparisonPostParser = reqparse.RequestParser()
comparisonPostParser.add_argument('projectId', type=int, location='json')
comparisonPostParser.add_argument('spec', type=dict, location='json')
comparisonPostParser.add_argument('conditionals', type=dict, location='json', default={})
class ComparisonFromSpec(Resource):
    def post(self):
        '''
        spec: {
            independentVariables
            dependentVariable
            interactionTerms
            model
            estimator
            degree
            weights
            functions
            datasetId
            tableLayout
        }
        '''

        args = comparisonPostParser.parse_args()
        project_id = args.get('projectId')
        spec = args.get('spec')
        conditionals = args.get('conditionals', {})

        comparison_doc = db_access.get_comparison_from_spec(project_id, spec, conditionals=conditionals)

        # check to see if comparison is in db; if so, send back data
        if comparison_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            comparison_data = comparison_doc['data']
            comparison_data['id'] = comparison_doc['id']

            exported_comparison_doc = db_access.get_exported_comparison_by_comparison_id(project_id, comparison_doc['id'])
            if exported_comparison_doc:
                comparison_data['exported'] = True
                comparison_data['exportedComparisonId'] = exported_comparison_doc['id']
            else:
                comparison_data['exported'] = False
            return jsonify(comparison_data)
        else:
            comparison_task = comparison_pipeline.apply_async(
                args = [ spec, project_id, conditionals ]
            )

            return jsonify({
                'task_id': comparison_task.task_id,
                'compute': True
            }, status=202)


summaryPostParser = reqparse.RequestParser()
summaryPostParser.add_argument('projectId', type=int, location='json')
summaryPostParser.add_argument('spec', type=dict, location='json')
class AggregationStatsFromSpec(Resource):
    def post(self):
        '''
        spec: {
            datasetId : integer
            fieldIds : list
        }
        '''
        args = summaryPostParser.parse_args()
        project_id = args.get('projectId')
        spec = args.get('spec')

        summary_doc = db_access.get_aggregation_from_spec(project_id, spec, conditionals=conditionals)
        if summary_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            summary_data = summary_doc['data']
            summary_data['id'] = summary_doc['id']
            return jsonify(summary_data)
        else:
            summary_task = summary_pipeline.apply_async(
                args = [spec, project_id, conditionals]
            )

            return jsonify({
                'task_id': summary_task.task_id,
                'compute': True
            }, status=202)

oneDimensionalTableFromSpecPostParser = reqparse.RequestParser()
oneDimensionalTableFromSpecPostParser.add_argument('projectId', type=int, location='json')
oneDimensionalTableFromSpecPostParser.add_argument('spec', type=dict, location='json')
oneDimensionalTableFromSpecPostParser.add_argument('conditionals', type=dict, location='json', default={})
class OneDimensionalTableFromSpec(Resource):
    def post(self):
        '''
        spec: {
            dataset_id
            categoricalIndependentVariableNames
            numericalIndependentVariableNames
            dependentVariable
        }
        '''
        args = oneDimensionalTableFromSpecPostParser.parse_args()
        project_id = args.get('projectId')
        spec = args.get('spec')
        conditionals = args.get('conditionals')

        table_doc = db_access.get_aggregation_from_spec(project_id, spec, conditionals=conditionals)
        if table_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            table_data = table_doc['data']
            table_data['id'] = table_doc['id']
            return jsonify(table_data)
        else:
            table_task = one_dimensional_contingency_table_pipeline.apply_async(
                args = [spec, project_id, conditionals]
            )
            return jsonify({
                'task_id': table_task.task_id,
                'compute': True
            }, status=202)


contingencyTableFromSpecPostParser = reqparse.RequestParser()
contingencyTableFromSpecPostParser.add_argument('projectId', type=int, location='json')
contingencyTableFromSpecPostParser.add_argument('spec', type=dict, location='json')
contingencyTableFromSpecPostParser.add_argument('conditionals', type=dict, location='json', default={})
class ContingencyTableFromSpec(Resource):
    def post(self):
        '''
        spec: {
            datasetId
            categoricalIndependentVariableNames
            numericalIndependentVariableNames
            dependentVariable
        }
        '''
        args = contingencyTableFromSpecPostParser.parse_args()
        project_id = args.get('projectId')
        spec = args.get('spec')
        conditionals = args.get('conditionals', {})

        table_doc = db_access.get_aggregation_from_spec(project_id, spec, conditionals=conditionals)

        if table_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            table_data = table_doc['data']
            table_data['id'] = table_doc['id']
            return jsonify(table_data)
        else:
            table_task = contingency_table_pipeline.apply_async(
                args = [spec, project_id, conditionals]
            )
            return jsonify({
                'task_id': table_task.task_id,
                'compute': True
            }, status=202)


correlationsFromSpecPostParser = reqparse.RequestParser()
correlationsFromSpecPostParser.add_argument('projectId', type=int, location='json')
correlationsFromSpecPostParser.add_argument('spec', type=dict, location='json')
correlationsFromSpecPostParser.add_argument('conditionals', type=dict, location='json', default={})
class CorrelationsFromSpec(Resource):
    def post(self):
        '''
        spec: {
            datasetId
            correlationVariables
        }
        '''
        args = correlationsFromSpecPostParser.parse_args()
        project_id = args.get('projectId')
        spec = args.get('spec')
        conditionals = args.get('conditionals')

        correlation_doc = db_access.get_correlation_from_spec(project_id, spec, conditionals=conditionals)
        if correlation_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            correlation_data = correlation_doc['data']
            correlation_data['id'] = correlation_doc['id']

            exported_correlation_doc = db_access.get_exported_correlation_by_correlation_id(project_id, correlation_doc['id'])
            if exported_correlation_doc:
                correlation_data['exported'] = True
                correlation_data['exportedCorrelationId'] = exported_correlation_doc['id']
            else:
                correlation_data['exported'] = False
            return jsonify(correlation_data)
        else:
            correlation_task = correlation_pipeline.apply_async(
                args = [spec, project_id, conditionals]
            )

            return jsonify({
                'task_id': correlation_task.task_id,
                'compute': True
            }, status=202)
