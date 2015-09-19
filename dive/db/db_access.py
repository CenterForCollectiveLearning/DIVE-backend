'''
Module containing functions accessing the database. Other should have no direct
access to the database, only to this layer. Parameters in, JSONable objects out.

Mainly used to separate session management from models, and to provide uniform
db interfaces to both the API and compute layers.

TODO Have a general decorator argument
'''

from models import *
from dive.core import db

import logging
logger = logging.getLogger(__name__)

def row_to_dict(r):
    return {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}

################
# Projects
# https://github.com/sloria/PythonORMSleepy/blob/master/sleepy/api_sqlalchemy.py
################
def get_project(project_id):
    project = Project.query.get_or_404(int(project_id))
    return row_to_dict(project)

def get_projects(**kwargs):
    projects = Project.query.filter_by(**kwargs).all()
    return [ row_to_dict(project) for project in projects ]

def insert_project(**kwargs):
    title = kwargs.get('title')
    description = kwargs.get('description')

    project = Project(
        title=title,
        description=description,
        creation_date=datetime.utcnow()
    )
    db.session.add(project)
    db.session.commit()
    return row_to_dict(project)

def update_project(project_id, **kwargs):
    title = kwargs.get('title')
    description = kwargs.get('description')

    project = Project.query.get_or_404(int(project_id))
    if kwargs.get('title'): project.title = title
    if kwargs.get('description'): project.description = description

    project = Project(title=title, description=description)
    db.session.add(project)
    db.session.commit()
    return row_to_dict(project)

def delete_project(project_id):
    project = Project.query.get_or_404(int(project_id))
    db.session.delete(project)
    db.session.commit()
    return row_to_dict(project)

################
# Datasets
################
def get_dataset(project_id, dataset_id):
    # http://stackoverflow.com/questions/2128505/whats-the-difference-between-filter-and-filter-by-in-sqlalchemy
    logger.info("Get dataset with project_id %s and dataset_id %s", project_id, dataset_id)
    dataset = Dataset.query.filter_by(project_id=project_id, id=dataset_id).one()
    return row_to_dict(dataset)

def get_datasets(**kwargs):
    datasets = Dataset.query.filter_by(**kwargs).all()
    return [ row_to_dict(dataset) for dataset in datasets ]

def insert_dataset(project_id, **kwargs):
    logger.info("Insert dataset with project_id %s", project_id)
    title = kwargs.get('title')
    file_name = kwargs.get('file_name')
    path = kwargs.get('path')
    file_type = kwargs.get('type')
    orig_type = kwargs.get('orig_type')

    # TODO Unpack these programmatically?
    dataset = Dataset(
        title = title,
        type = file_type,
        orig_type = orig_type,
        file_name = file_name,
        path = path,
        project_id = project_id
    )
    logger.info(dataset)
    db.session.add(dataset)
    db.session.commit()
    return row_to_dict(dataset)


def delete_dataset(project_id, dataset_id):
    dataset = Dataset.query.filter_by(project_id=project_id, id=dataset_id).one()
    db.session.delete(dataset)
    db.session.commit()
    return row_to_dict(dataset)


################
# Dataset Properties
################
def get_dataset_properties(project_id, dataset_id):
    dp = Dataset_Properties.query.filter_by(project_id=project_id, id=dataset_id).all()
    return [ row_to_dict(dp) for dp in dp ]

# TODO Do an upsert?
def insert_dataset_properties(project_id, dataset_id, **kwargs):
    logger.info("Insert data properties with project_id %s, and dataset_id %s", project_id, dataset_id)
    dataset_properties = Dataset_Properties(
        n_rows = kwargs.get('n_rows'),
        n_cols = kwargs.get('n_cols'),
        field_names = kwargs.get('field_names'),
        field_types = kwargs.get('field_types'),
        field_accessors = kwargs.get('field_accessors'),
        is_time_series = kwargs.get('is_time_series'),
        structure = kwargs.get('structure'),
        dataset_id = dataset_id,
        project_id = project_id,
    )
    db.session.add(dataset_properties)
    db.session.commit()
    return row_to_dict(dataset_properties)


def delete_dataset_properties(project_id, dataset_id):
    dataset_properties = Dataset_Properties.query.filter_by(project_id=project_id, id=dataset_id).one()
    db.session.delete(dataset_properties)
    db.session.commit()
    return row_to_dict(dataset_properties)

################
# Field Properties
################
def get_field_properties(project_id, dataset_id):
    dp = Field_Properties.query.filter_by(project_id=project_id, id=dataset_id).all()
    return [ row_to_dict(dp) for dp in dp ]

# TODO Do an upsert?
def insert_field_properties(project_id, dataset_id, **kwargs):
    logger.info("Insert data properties with project_id %s, and dataset_id %s", project_id, dataset_id)
    dataset_properties = Dataset_Properties(
        # n_rows = kwargs.get('n_rows')
        # n_cols
        # field_names = kwargs.get('field_names')
        # field_types
        # field_accessors =
        # is_time_series = kwargs.get('time_series')
        # structure = kwargs.get('structure')
        # dataset_id = kwargs.get('dataset_id')
        # project_id = kwargs.get('project_id')
    )
    db.session.add(dataset_properties)
    db.session.commit()
    return row_to_dict(dataset_properties)


def delete_field_properties(project_id, dataset_id):
    dataset_properties = Dataset_Properties.query.filter_by(project_id=project_id, id=dataset_id).one()
    db.session.delete(dataset_properties)
    db.session.commit()
    return row_to_dict(dataset_properties)

################
# Specifications
################
def insert_specs(project_id, specs):
    spec_objects = []
    for s in specs:
        spec_objects.append(Spec(
            s
        ))
    db.session.add_all(spec_objects)
    db.session.commit()

def delete_spec(project_id, exported_spec_id):
    spec = Exported_Spec.query.filter_by(project_id=project_id, id=exported_spec_id).one()
    if exported_spec:
        db.session.delete(spec)
        db.session.commit()
        return row_to_dict(spec)

################
# Exported Specifications
################
def insert_exported_spec(project_id, exported_spec):
    exported_spec = Exported_Spec(
    )
    db.session.add_all(exported_spec)
    db.session.commit()
    return row_to_dict(exported_spec)

def delete_exported_spec(project_id, exported_spec_id):
    exported_spec = Exported_Spec.query.filter_by(project_id=project_id, id=exported_spec_id).one()
    if exported_spec:
        db.session.delete(exported_spec)
        db.session.commit()
        return row_to_dict(exported_spec)
