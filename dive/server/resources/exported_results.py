from flask_restful import Resource, reqparse

from dive.base.db import db_access
from dive.base.serialization import jsonify

import logging
logger = logging.getLogger(__name__)


exportedResultsGetParser = reqparse.RequestParser()
exportedResultsGetParser.add_argument('project_id', type=int, required=True)
exportedResultsGetParser.add_argument('result_type', type=str, action='append')
class ExportedResults(Resource):
    def get(self):
        args = exportedResultsGetParser.parse_args()
        project_id = args.get('project_id')
        result_types = args.get('result_type')

        exported_results = []
        if 'summary' in result_types:
            exported_summaries = db_access.get_exported_summaries(project_id)
            exported_results.extend(exported_summaries)
        if 'correlation' in result_types:
            exported_correlations = db_access.get_exported_correlations(project_id)
            exported_results.extend(exported_correlations)
        if 'regression' in result_types:
            exported_regressions = db_access.get_exported_regressions(project_id)
            exported_results.extend(exported_regressions)

        return jsonify({'result': exported_results, 'length': len(exported_results)})

    def post(self):
        args = exportedResultsPostParser.parse_args()
        project_id = args.get('project_id')
        regression_id = args.get('regression_id')
        data = args.get('data')

        result = db_access.insert_exported_regression(project_id, regression_id, data)
        return jsonify(result)
