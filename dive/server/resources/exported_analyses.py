from flask_restful import Resource, reqparse

from dive.base.db import db_access
from dive.base.serialization import jsonify

import logging
logger = logging.getLogger(__name__)


exportedAnalysesGetParser = reqparse.RequestParser()
exportedAnalysesGetParser.add_argument('project_id', type=int, required=True)
exportedAnalysesGetParser.add_argument('result_types', type=dict, default=[ 'aggregation', 'comparison', 'correlation', 'regression' ])
class ExportedAnalyses(Resource):
    def get(self):
        args = exportedAnalysesGetParser.parse_args()
        project_id = args.get('project_id')
        result_types = args.get('result_types')

        exported_results = {}
        if 'aggregation' in result_types:
            exported_results['aggregation'] = db_access.get_exported_aggregations(project_id)
        if 'comparison' in result_types:
            exported_results['comparison'] = db_access.get_exported_comparisons(project_id)         
        if 'correlation' in result_types:
            exported_results['correlation'] = db_access.get_exported_correlations(project_id)
        if 'regression' in result_types:
            exported_results['regression'] = db_access.get_exported_regressions(project_id)

        return jsonify(exported_results)


exportedRegressionGetParser = reqparse.RequestParser()
exportedRegressionGetParser.add_argument('project_id', type=int, required=True)

exportedRegressionPostParser = reqparse.RequestParser()
exportedRegressionPostParser.add_argument('project_id', type=int, required=True, location='json')
exportedRegressionPostParser.add_argument('regression_id', type=int, required=True, location='json')
exportedRegressionPostParser.add_argument('data', type=dict, required=True, location='json')
exportedRegressionPostParser.add_argument('conditionals', type=dict, required=True, location='json')
exportedRegressionPostParser.add_argument('config', type=dict, required=True, location='json')
class ExportedRegression(Resource):
    def get(self):
        args = exportedRegressionGetParser.parse_args()
        project_id = args.get('project_id')

        exported_regressions = db_access.get_exported_regression(project_id)
        return jsonify({ 'result': exported_regressions })

    def post(self):
        args = exportedRegressionPostParser.parse_args()
        project_id = args.get('project_id')
        regression_id = args.get('regression_id')
        data = args.get('data')
        conditionals = args.get('conditionals')
        config = args.get('config')

        result = db_access.insert_exported_regression(project_id, regression_id, data, conditionals, config)
        result['spec'] = db_access.get_regression_by_id(regression_id, project_id)['spec']
        return jsonify(result)


dataFromExportedRegressionGetParser = reqparse.RequestParser()
dataFromExportedRegressionGetParser.add_argument('project_id', type=int, required=True)
class DataFromExportedRegression(Resource):
    def get(self, exported_regression_id):
        args = dataFromExportedRegressionGetParser.parse_args()
        project_id = args.get('project_id')

        exported_regression = db_access.get_exported_regression(project_id, exported_regression_id)
        regression_id = exported_spec['regression_id']
        regression = db_access.get_regression_by_id(regression_id, project_id)

        return jsonify(regression['data'])



exportedCorrelationGetParser = reqparse.RequestParser()
exportedCorrelationGetParser.add_argument('project_id', type=int, required=True)

exportedCorrelationPostParser = reqparse.RequestParser()
exportedCorrelationPostParser.add_argument('project_id', type=int, required=True, location='json')
exportedCorrelationPostParser.add_argument('correlation_id', type=int, required=True, location='json')
exportedCorrelationPostParser.add_argument('data', type=dict, required=True, location='json')
exportedCorrelationPostParser.add_argument('conditionals', type=dict, required=True, location='json')
exportedCorrelationPostParser.add_argument('config', type=dict, required=True, location='json')
class ExportedCorrelation(Resource):
    def get(self):
        args = exportedCorrelationGetParser.parse_args()
        project_id = args.get('project_id')

        exported_correlation = db_access.get_exported_correlation(project_id)
        return jsonify({ 'result': exported_correlation })

    def post(self):
        args = exportedCorrelationPostParser.parse_args()
        project_id = args.get('project_id')
        correlation_id = args.get('correlation_id')
        data = args.get('data')
        conditionals = args.get('conditionals')
        config = args.get('config')

        result = db_access.insert_exported_correlation(project_id, correlation_id, data, conditionals, config)
        result['spec'] = db_access.get_correlation_by_id(correlation_id, project_id)['spec']
        return jsonify(result)


dataFromExportedCorrelationGetParser = reqparse.RequestParser()
dataFromExportedCorrelationGetParser.add_argument('project_id', type=int, required=True)
class DataFromExportedCorrelation(Resource):
    def get(self, exported_correlation_id):
        args = dataFromExportedCorrelationGetParser.parse_args()
        project_id = args.get('project_id')

        exported_correlation = db_access.get_exported_correlation(project_id, exported_correlation_id)
        correlation_id = exported_spec['correlation_id']
        correlation = db_access.get_correlation_by_id(correlation_id, project_id)

        return jsonify(correlation['data'])


exportedAggregationGetParser = reqparse.RequestParser()
exportedAggregationGetParser.add_argument('project_id', type=int, required=True)

exportedAggregationPostParser = reqparse.RequestParser()
exportedAggregationPostParser.add_argument('project_id', type=int, required=True, location='json')
exportedAggregationPostParser.add_argument('aggregation_id', type=int, required=True, location='json')
exportedAggregationPostParser.add_argument('data', type=dict, required=True, location='json')
exportedAggregationPostParser.add_argument('conditionals', type=dict, required=True, location='json')
exportedAggregationPostParser.add_argument('config', type=dict, required=True, location='json')
class ExportedAggregation(Resource):
    def get(self):
        args = exportedAggregationGetParser.parse_args()
        project_id = args.get('project_id')

        exported_aggregations = db_access.get_exported_aggregations(project_id)
        return jsonify({'result': exported_aggregations, 'length': len(exported_aggregations)})

    def post(self):
        args = exportedAggregationPostParser.parse_args()
        project_id = args.get('project_id')
        aggregation_id = args.get('aggregation_id')
        data = args.get('data')
        conditionals = args.get('conditionals')
        config = args.get('config')

        result = db_access.insert_exported_aggregation(project_id, aggregation_id, data, conditionals, config)
        result['spec'] = db_access.get_aggregation_by_id(aggregation_id, project_id)['spec']
        return jsonify(result)


dataFromExportedAggregationGetParser = reqparse.RequestParser()
dataFromExportedAggregationGetParser.add_argument('project_id', type=int, required=True)
class DataFromExportedAggregation(Resource):
    def get(self, exported_correlation_id):
        args = dataFromExportedAggregationGetParser.parse_args()
        project_id = args.get('project_id')

        exported_correlation = db_access.get_exported_aggregation(project_id, exported_aggregation_id)
        aggregation_id = exported_spec['aggregation_id']
        aggregation = db_access.get_aggregation_by_id(aggregation_id, project_id)

        return jsonify(summary['data'])


exportedComparisonGetParser = reqparse.RequestParser()
exportedComparisonGetParser.add_argument('project_id', type=int, required=True)

exportedComparisonPostParser = reqparse.RequestParser()
exportedComparisonPostParser.add_argument('project_id', type=int, required=True, location='json')
exportedComparisonPostParser.add_argument('comparison_id', type=int, required=True, location='json')
exportedComparisonPostParser.add_argument('data', type=dict, required=True, location='json')
exportedComparisonPostParser.add_argument('conditionals', type=dict, required=True, location='json')
exportedComparisonPostParser.add_argument('config', type=dict, required=True, location='json')
class ExportedComparison(Resource):
    def get(self):
        args = exportedComparisonGetParser.parse_args()
        project_id = args.get('project_id')

        exported_comparisons = db_access.get_exported_comparisons(project_id)
        return jsonify({'result': exported_comparisons, 'length': len(exported_comparisons)})

    def post(self):
        args = exportedComparisonPostParser.parse_args()
        project_id = args.get('project_id')
        comparison_id = args.get('comparison_id')
        data = args.get('data')
        conditionals = args.get('conditionals')
        config = args.get('config')

        result = db_access.insert_exported_comparison(project_id, comparison_id, data, conditionals, config)
        result['spec'] = db_access.get_comparison_by_id(comparison_id, project_id)['spec']
        return jsonify(result)


dataFromExportedComparisonGetParser = reqparse.RequestParser()
dataFromExportedComparisonGetParser.add_argument('project_id', type=int, required=True)
class DataFromExportedComparison(Resource):
    def get(self, exported_correlation_id):
        args = dataFromExportedComparisonGetParser.parse_args()
        project_id = args.get('project_id')

        exported_correlation = db_access.get_exported_comparison(project_id, exported_comparison_id)
        comparison_id = exported_spec['comparison_id']
        comparison = db_access.get_comparison_by_id(comparison_id, project_id)

        return jsonify(summary['data'])
