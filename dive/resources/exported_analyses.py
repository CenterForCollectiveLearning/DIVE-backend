from flask_restful import Resource, reqparse

from dive.db import db_access
from dive.resources.serialization import jsonify

import logging
logger = logging.getLogger(__name__)


def object_type(j):
    return j


exportedRegressionGetParser = reqparse.RequestParser()
exportedRegressionGetParser.add_argument('project_id', type=str, required=True)

exportedRegressionPostParser = reqparse.RequestParser()
exportedRegressionPostParser.add_argument('project_id', type=str, required=True, location='json')
exportedRegressionPostParser.add_argument('regression_id', type=str, required=True, location='json')
exportedRegressionPostParser.add_argument('data', type=object_type, required=True, location='json')
exportedRegressionPostParser.add_argument('conditionals', type=object_type, required=True, location='json')
exportedRegressionPostParser.add_argument('config', type=object_type, required=True, location='json')
class ExportedRegression(Resource):
    def get(self):
        args = exportedRegressionGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

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
dataFromExportedRegressionGetParser.add_argument('project_id', type=str, required=True)
class DataFromExportedRegression(Resource):
    def get(self, exported_regression_id):
        args = dataFromExportedRegressionGetParser.parse_args()
        project_id = args.get('project_id')

        exported_regression = db_access.get_exported_regression(project_id, exported_regression_id)
        regression_id = exported_spec['regression_id']
        regression = db_access.get_regression_by_id(regression_id, project_id)

        return jsonify(regression['data'])


exportedCorrelationGetParser = reqparse.RequestParser()
exportedCorrelationGetParser.add_argument('project_id', type=str, required=True)

exportedCorrelationPostParser = reqparse.RequestParser()
exportedCorrelationPostParser.add_argument('project_id', type=str, required=True, location='json')
exportedCorrelationPostParser.add_argument('correlation_id', type=str, required=True, location='json')
exportedCorrelationPostParser.add_argument('data', type=object_type, required=True, location='json')
exportedCorrelationPostParser.add_argument('conditionals', type=object_type, required=True, location='json')
exportedCorrelationPostParser.add_argument('config', type=object_type, required=True, location='json')
class ExportedCorrelation(Resource):
    def get(self):
        args = exportedCorrelationGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

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
dataFromExportedCorrelationGetParser.add_argument('project_id', type=str, required=True)
class DataFromExportedCorrelation(Resource):
    def get(self, exported_correlation_id):
        args = dataFromExportedCorrelationGetParser.parse_args()
        project_id = args.get('project_id')

        exported_correlation = db_access.get_exported_correlation(project_id, exported_correlation_id)
        correlation_id = exported_spec['correlation_id']
        correlation = db_access.get_correlation_by_id(correlation_id, project_id)

        return jsonify(correlation['data'])


exportedAggregationGetParser = reqparse.RequestParser()
exportedAggregationGetParser.add_argument('project_id', type=str, required=True)

exportedAggregationPostParser = reqparse.RequestParser()
exportedAggregationPostParser.add_argument('project_id', type=str, required=True, location='json')
exportedAggregationPostParser.add_argument('summary_id', type=str, required=True, location='json')
exportedAggregationPostParser.add_argument('conditionals', type=dict, required=True, location='json')
exportedAggregationPostParser.add_argument('config', type=dict, required=True, location='json')
class ExportedAggregation(Resource):
    def get(self):
        args = exportedAggregationGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        exported_summarys = db_access.get_exported_regressions(project_id)
        return jsonify({'result': exported_regressions, 'length': len(exported_summarys)})

    def post(self):
        args = exportedAggregationPostParser.parse_args()
        project_id = args.get('project_id')
        summary_id = args.get('summary_id')
        data = args.get('data')
        conditionals = args.get('conditionals')
        config = args.get('config')

        result = db_access.insert_exported_summary(project_id, summary_id, conditionals, config)
        result['spec'] = db_access.get_aggregation_by_id(summary_id, project_id)['spec'] 
        return jsonify(result)


dataFromExportedAggregationGetParser = reqparse.RequestParser()
dataFromExportedAggregationGetParser.add_argument('project_id', type=str, required=True)
class DataFromExportedAggregation(Resource):
    def get(self, exported_correlation_id):
        args = dataFromExportedAggregationGetParser.parse_args()
        project_id = args.get('project_id')

        exported_correlation = db_access.get_exported_summary(project_id, exported_summary_id)
        summary_id = exported_spec['summary_id']
        summary = db_access.get_aggregation_by_id(summary_id, project_id)

        return jsonify(summary['data'])
