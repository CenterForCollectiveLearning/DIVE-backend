from flask.ext.restful import Resource, reqparse

from dive.db import db_access
from dive.resources.serialization import jsonify

import logging
logger = logging.getLogger(__name__)


exportedRegressionsGetParser = reqparse.RequestParser()
exportedRegressionsGetParser.add_argument('project_id', type=str, required=True)

exportedRegressionsPostParser = reqparse.RequestParser()
exportedRegressionsPostParser.add_argument('project_id', type=str, required=True, location='json')
exportedRegressionsPostParser.add_argument('regression_id', type=str, required=True, location='json')
class ExportedRegressions(Resource):
    def get(self):
        args = exportedRegressionsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        exported_regressions = db_access.get_exported_regressions(project_id)
        return jsonify({'result': exported_regressions, 'length': len(exported_regressions)})

    def post(self):
        args = exportedRegressionsPostParser.parse_args()
        project_id = args.get('project_id')
        regression_id = args.get('regression_id')

        result = db_access.insert_exported_regression(project_id, regression_id)
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


exportedCorrelationsGetParser = reqparse.RequestParser()
exportedCorrelationsGetParser.add_argument('project_id', type=str, required=True)

exportedCorrelationsPostParser = reqparse.RequestParser()
exportedCorrelationsPostParser.add_argument('project_id', type=str, required=True, location='json')
exportedCorrelationsPostParser.add_argument('regression_id', type=str, required=True, location='json')
class ExportedCorrelations(Resource):
    def get(self):
        args = exportedCorrelationsGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        exported_regressions = db_access.get_exported_regressions(project_id)
        return jsonify({'result': exported_regressions, 'length': len(exported_regressions)})

    def post(self):
        args = exportedCorrelationsPostParser.parse_args()
        project_id = args.get('project_id')
        regression_id = args.get('regression_id')

        result = db_access.insert_exported_regression(project_id, regression_id)
        return jsonify(result)


dataFromExportedCorrelationGetParser = reqparse.RequestParser()
dataFromExportedCorrelationGetParser.add_argument('project_id', type=str, required=True)
class DataFromExportedCorrelations(Resource):
    def get(self, exported_correlation_id):
        args = dataFromExportedCorrelationGetParser.parse_args()
        project_id = args.get('project_id')

        exported_correlation = db_access.get_exported_correlation(project_id, exported_correlation_id)
        correlation_id = exported_spec['correlation_id']
        correlation = db_access.get_correlation_by_id(correlation_id, project_id)

        return jsonify(correlation['data'])


exportedSummaryGetParser = reqparse.RequestParser()
exportedSummaryGetParser.add_argument('project_id', type=str, required=True)

exportedSummaryPostParser = reqparse.RequestParser()
exportedSummaryPostParser.add_argument('project_id', type=str, required=True, location='json')
exportedSummaryPostParser.add_argument('regression_id', type=str, required=True, location='json')
class ExportedSummary(Resource):
    def get(self):
        args = exportedSummaryGetParser.parse_args()
        project_id = args.get('project_id').strip().strip('"')

        exported_summarys = db_access.get_exported_regressions(project_id)
        return jsonify({'result': exported_regressions, 'length': len(exported_summarys)})

    def post(self):
        args = exportedSummaryPostParser.parse_args()
        project_id = args.get('project_id')
        summary_id = args.get('summary_id')

        result = db_access.insert_exported_summary(project_id, summary_id)
        return jsonify(result)


dataFromExportedSummaryGetParser = reqparse.RequestParser()
dataFromExportedSummaryGetParser.add_argument('project_id', type=str, required=True)
class DataFromExportedSummary(Resource):
    def get(self, exported_correlation_id):
        args = dataFromExportedSummaryGetParser.parse_args()
        project_id = args.get('project_id')

        exported_correlation = db_access.get_exported_summary(project_id, exported_summary_id)
        summary_id = exported_spec['summary_id']
        summary = db_access.get_summary_by_id(summary_id, project_id)

        return jsonify(summary['data'])
