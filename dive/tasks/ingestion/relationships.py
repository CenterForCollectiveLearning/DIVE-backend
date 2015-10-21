from itertools import combinations

from dive.db import db_access
from dive.task_core import celery, task_app
from dive.data.access import get_data

from celery import states
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


# Find the distance between two sets
# Currently naively uses Jaccard distance between two sets
def get_distance(list_a, list_b):
    set_a, set_b = set(list_a), set(list_b)
    return float(len(set_a.intersection(set_b))) / len(set_a.union(set_b))


THRESHOLD = task_app.config['FIELD_RELATIONSHIP_DISTANCE_THRESHOLD']
@celery.task(bind=True, ignore_result=True, task_name='compute_relationships')
def compute_relationships(self, project_id):
    with task_app.app_context():
        all_datasets = db_access.get_datasets(project_id)
    relationships = []

    if len(all_datasets) == 1:
        return relationships

    for dataset_a, dataset_b in combinations(all_datasets, 2):
        with task_app.app_context():
            dataset_a_fields = db_access.get_field_properties(project_id, dataset_a['id'])
            dataset_b_fields = db_access.get_field_properties(project_id, dataset_b['id'])

        for index_a, field_a in enumerate(dataset_a_fields):
            for index_b, field_b in enumerate(dataset_b_fields):
                logger.info('%s:%s - %s:%s', dataset_a['title'], field_a['name'], dataset_b['title'], field_b['name'])
                unique_field_a_values = field_a.get('unique_values')
                unique_field_b_values = field_b.get('unique_values')

                if (not unique_field_a_values) or (not unique_field_b_values):
                    continue

                len_a = len(unique_field_a_values)
                len_b = len(unique_field_b_values)

                d = get_distance(unique_field_a_values, unique_field_b_values)
                logger.info('%s-%s: %s', field_a['name'], field_b['name'], d)

                if d >= THRESHOLD:
                    if len_a == len_b:
                        relationship_type = "11"
                    elif (len_a > len_b):
                        relationship_type = "N1"
                    elif (len_a < len_a):
                        relationship_type = "1N"
                    else:
                        relationship_type = None
                else:
                    continue

                relationship = {
                    'source_dataset_id': dataset_a['id'],
                    'source_field_id': field_a['id'],
                    'target_dataset_id': dataset_b['id'],
                    'target_field_id': field_b['id'],
                    'source_dataset_name': dataset_a['title'],
                    'source_field_name': field_a['name'],
                    'target_dataset_name': dataset_b['title'],
                    'target_field_name': field_b['name'],
                    'distance': d,
                    'type': relationship_type
                }
                relationships.append(relationship)

        return relationships


@celery.task(bind=True, ignore_result=True)
def save_relationships(self, relationships, project_id):
    self.update_state(state=states.PENDING, meta={'status': 'Saving relationships'})
    with task_app.app_context():
        db_access.insert_relationships(relationships, project_id)
    self.update_state(state=states.SUCCESS, meta={'status': 'Saved relationships'})
