'''
Module containing functions accessing the database.

Other should have no direct access to the database, only to this layer.
Parameters in, JSONable objects out.
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


def insert_dataset(project_id, **kwargs):
    logger.info("Insert dataset with project_id %s", project_id)
    title = kwargs.get('title')
    file_name = kwargs.get('file_name')
    path = kwargs.get('path')


    # TODO Unpack these programmatically?
    dataset = Dataset(
        title = title,
        file_name = file_name,
        path = path,
        project_id = project_id
    )
    logger.info(dataset)
    db.session.add(dataset)
    db.session.commit()
    return row_to_dict(dataset)


def delete_project(project_id, dataset_id):
    dataset = Dataset.query.filter_by(project_id=project_id, id=dataset_id).one()
    db.session.delete(dataset)
    db.session.commit()
    return row_to_dict(dataset)


def get_dataset_properties(project_id, dataset_id):
    dp = Dataset_Properties.query.filter_by(project_id=project_id, id=dataset_id).all()
    return [ row_to_dict(dp) for dp in dp ]


# TODO Do an upsert?
def insert_dataset_properties(project_id, dataset_id, **kwargs):
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

################
# Dataset Properties
################

################
# Field Properties
################

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

################
# Exported Specifications
################

################
# Users
################

#####################

model_from_name = {
    'Project': Project,
}

def get_objects(project_id, model_name, **kwargs):
    model = model_from_name[model_name]
    model.query.filter_by(**kwargs).all()

# TODO Upsert?

def update_objects(project_id, model_name, **kwargs):
    model = model_from_name[model_name]
    model.query.filter_by(**kwargs).all()

def insert_objects(project_id, model_name, **kwargs):
    model = model_from_name[model_name]
    model.query.filter_by(**kwargs).all()

# Cascade
def delete_objects(project_ids = None):
    # Synchronize session?
    # How to deal with the filter object?

    model = model_from_name[model_name]

    for project_id in project_ids:
        model.query.filter_by(**kwargs).delete()
    db.session.commit()
