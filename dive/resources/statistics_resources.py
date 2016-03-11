import time
from flask import current_app, request, make_response
from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.serialization import jsonify

from dive.tasks.statistics.regression import get_contribution_to_r_squared_data
from dive.tasks.statistics.correlation import get_correlation_scatterplot_data
from dive.tasks.pipelines import regression_pipeline, summary_pipeline, correlation_pipeline, one_dimensional_contingency_table_pipeline, contingency_table_pipeline
from dive.tasks.handlers import error_handler

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


contributionToRSquaredGetParser = reqparse.RequestParser()
contributionToRSquaredGetParser.add_argument('projectId', type=str)
class ContributionToRSquared(Resource):
    def get(self, regression_id):
        args = contributionToRSquaredGetParser.parse_args()
        project_id = args.get('projectId')
        regression_doc = db_access.get_regression_by_id(regression_id, project_id)
        regression_data = regression_doc['data']
        data = get_contribution_to_r_squared_data(regression_data)
        logger.info(data)
        return jsonify({ 'data': data })


#####################################################################
# Endpoint returning regression data given a specification
# INPUT: project_id, spec
# OUTPUT: {stat data}
#####################################################################
regressionPostParser = reqparse.RequestParser()
regressionPostParser.add_argument('projectId', type=str, location='json')
regressionPostParser.add_argument('spec', type=dict, location='json')
class RegressionFromSpec(Resource):
    def post(self):
        '''
        spec: {
            independentVariables
            dependentVariable
            model
            estimator
            degree
            weights
            functions
            datasetId
        }
        '''
        args = regressionPostParser.parse_args()
        project_id = args.get('projectId')
        spec = args.get('spec')

        regression_doc = db_access.get_regression_from_spec(project_id, spec)
        if regression_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            regression_data = regression_doc['data']
            regression_data['id'] = regression_doc['id']
            return jsonify(regression_data)
        else:
            regression_task = regression_pipeline.apply_async(
                args = [spec, project_id],
                link_error = error_handler.s()
            )

            return jsonify({
                'task_id': regression_task.task_id,
                'compute': True
            }, status=202)

summaryPostParser = reqparse.RequestParser()
summaryPostParser.add_argument('projectId', type=str, location='json')
summaryPostParser.add_argument('spec', type=dict, location='json')
class SummaryStatsFromSpec(Resource):
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

        summary_doc = db_access.get_summary_from_spec(project_id, spec)
        if summary_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            summary_data = summary_doc['data']
            summary_data['id'] = summary_doc['id']
            return jsonify(summary_data)
        else:
            summary_task = summary_pipeline.apply_async(
                args = [spec, project_id],
                link_error = error_handler.s()
            )

            return jsonify({
                'task_id': summary_task.task_id,
                'compute': True
            }, status=202)

oneDimensionalTableFromSpecPostParser = reqparse.RequestParser()
oneDimensionalTableFromSpecPostParser.add_argument('projectId', type=str, location='json')
oneDimensionalTableFromSpecPostParser.add_argument('spec', type=dict, location='json')
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

        table_doc = db_access.get_summary_from_spec(project_id, spec)
        if table_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            table_data = table_doc['data']
            table_data['id'] = table_doc['id']
            return jsonify(table_data)
        else:
            table_task = one_dimensional_contingency_table_pipeline.apply_async(
                args = [spec, project_id],
                link_error = error_handler.s()
            )
            return jsonify({
                'task_id': table_task.task_id,
                'compute': True
            }, status=202)


contingencyTableFromSpecPostParser = reqparse.RequestParser()
contingencyTableFromSpecPostParser.add_argument('projectId', type=str, location='json')
contingencyTableFromSpecPostParser.add_argument('spec', type=dict, location='json')
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

        table_doc = db_access.get_summary_from_spec(project_id, spec)

        if table_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            table_data = table_doc['data']
            table_data['id'] = table_doc['id']
            return jsonify(table_data)
        else:
            table_task = contingency_table_pipeline.apply_async(
                args = [spec, project_id],
                link_error = error_handler.s()
            )
            return jsonify({
                'task_id': table_task.task_id,
                'compute': True
            }, status=202)


correlationsFromSpecPostParser = reqparse.RequestParser()
correlationsFromSpecPostParser.add_argument('projectId', type=str, location='json')
correlationsFromSpecPostParser.add_argument('spec', type=dict, location='json')
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

        correlation_doc = db_access.get_correlation_from_spec(project_id, spec)
        if correlation_doc and not current_app.config['RECOMPUTE_STATISTICS']:
            correlation_data = correlation_doc['data']
            correlation_data['id'] = correlation_doc['id']
            return jsonify(correlation_data)
        else:
            correlation_task = correlation_pipeline.apply_async(
                args = [spec, project_id],
                link_error = error_handler.s()
            )

            return jsonify({
                'task_id': correlation_task.task_id,
                'compute': True
            }, status=202)


correlationScatterplotGetParser = reqparse.RequestParser()
correlationScatterplotGetParser.add_argument('projectId', type=str)
class CorrelationScatterplot(Resource):
    def get(self, correlation_id):
        args = correlationScatterplotGetParser.parse_args()
        project_id = args.get('projectId')
        correlation_doc = db_access.get_correlation_by_id(correlation_id, project_id)
        correlation_spec = correlation_doc['spec']
        data = get_correlation_scatterplot_data(correlation_spec, project_id)
        return jsonify({ 'data': data })
