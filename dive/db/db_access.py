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
def get_datasets(**kwargs):
    return

################
# Dataset Properties
################

################
# Field Properties
################

################
# Specifications
################

################
# Exported Specifications
################

################
# Users
################

#####################

model_from_name = {
    'Project': Project,
    'Dataset_Properties': Dataset_Properties,
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
