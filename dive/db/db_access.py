'''
Module containing functions and Data Access Objects for accessing the database.
Parameters in, JSONable objects out.

Mainly used to separate session management from models, and to provide uniform
db interfaces to both the API and compute layers.

TODO Have a general decorator argument
'''

from flask import abort
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound

from dive.core import db
from dive.db.models import *

import logging
logger = logging.getLogger(__name__)

def row_to_dict(r):
    return { c.name: getattr(r, c.name) for c in r.__table__.columns }

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
    user_id = kwargs.get('user_id')

    project = Project(
        title=title,
        description=description,
        creation_date=datetime.utcnow(),
        user_id=user_id
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
    try:
        dataset = Dataset.query.filter_by(project_id=project_id, id=dataset_id).one()
        return row_to_dict(dataset)

    # TODO Decide between raising error and aborting with 404
    except NoResultFound, e:
        logger.error(e)
        return None

    except MultipleResultsFound, e:
        logger.error(e)
        raise e

def get_datasets(project_id, **kwargs):
    datasets = Dataset.query.filter_by(project_id=project_id, **kwargs).all()
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
    try:
        dataset_properties = Dataset_Properties.query.filter_by(project_id=project_id, dataset_id=dataset_id).one()
        return row_to_dict(dataset_properties)
    except NoResultFound, e:
        logger.error(e)
        return None
    except MultipleResultsFound, e:
        logger.error(e)
        raise e

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

# TODO Do an upsert?
def update_dataset_properties(project_id, dataset_id, **kwargs):
    logger.info("Insert data properties with project_id %s, and dataset_id %s", project_id, dataset_id)

    dataset_properties = Dataset_Properties.query.filter_by(project_id=project_id,
        dataset_id=dataset_id,
        ).one()

    if kwargs.get('n_rows'): dataset_properties.n_rows = kwargs.get('n_rows')
    if kwargs.get('n_cols'): dataset_properties.n_cols = kwargs.get('n_cols')
    if kwargs.get('field_names'): dataset_properties.field_names = kwargs.get('field_names')
    if kwargs.get('field_types'): dataset_properties.field_types = kwargs.get('field_types')
    if kwargs.get('field_accessors'): dataset_properties.field_accessors = kwargs.get('field_accessors')
    if kwargs.get('is_time_series'): dataset_properties.is_time_series = kwargs.get('is_time_series')
    if kwargs.get('structure'): dataset_properties.structure = kwargs.get('structure'),

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
#
# TODO Write functions dealing with one vs many field properties
################
def get_field_properties(project_id, dataset_id, **kwargs):
    # TODO Add in field for kwargs name
    filter_dict = kwargs
    filter_dict['project_id'] = project_id
    filter_dict['dataset_id'] = dataset_id
    result = Field_Properties.query.filter_by(**filter_dict).all()
    field_properties = [ row_to_dict(r) for r in result ]
    return field_properties


def insert_field_properties(project_id, dataset_id, **kwargs):
    field_properties = Field_Properties(
        name = kwargs.get('name'),
        type = kwargs.get('type'),
        index = kwargs.get('index'),
        normality = kwargs.get('normality'),
        is_unique = kwargs.get('is_unique'),
        unique_values = kwargs.get('unique_values'),
        is_child = kwargs.get('is_child'),
        child = kwargs.get('child'),
        stats = kwargs.get('stats'),
        dataset_id = dataset_id,
        project_id = project_id,
    )
    db.session.add(field_properties)
    db.session.commit()
    return row_to_dict(field_properties)


def update_field_properties(project_id, dataset_id, name, **kwargs):
    title = kwargs.get('title')
    description = kwargs.get('description')

    field_properties = Field_Properties.query.filter_by(project_id=project_id,
        dataset_id=dataset_id,
        name=name).one()

    if kwargs.get('name'): field_properties.name = kwargs.get('name')
    if kwargs.get('type'): field_properties.type = kwargs.get('type')
    if kwargs.get('index'): field_properties.index = kwargs.get('index')
    if kwargs.get('normality'): field_properties.normality = kwargs.get('normality')
    if kwargs.get('is_unique'): field_properties.is_unique = kwargs.get('is_unique')
    if kwargs.get('child'): field_properties.child = kwargs.get('child')
    if kwargs.get('child'): field_properties.child = kwargs.get('is_child')
    if kwargs.get('unique_values'): field_properties.unique_values = kwargs.get('unique_values')
    if kwargs.get('stats'): field_properties.stats = kwargs.get('stats')

    db.session.add(field_properties)
    db.session.commit()
    return row_to_dict(field_properties)


def delete_field_properties(project_id, dataset_id):
    dataset_properties = Dataset_Properties.query.filter_by(project_id=project_id, id=dataset_id).one()
    db.session.delete(dataset_properties)
    db.session.commit()
    return row_to_dict(dataset_properties)


################
# Specifications
################
def get_spec(spec_id, project_id, **kwargs):
    # TODO Add in field for kwargs name
    spec = Spec.query.filter_by(id=spec_id, project_id=project_id).one()
    if spec is None:
        abort(404)
    return row_to_dict(spec)

def get_specs(project_id, dataset_id, **kwargs):
    # TODO Add in field for kwargs name
    specs = Spec.query.filter_by(project_id=project_id, dataset_id=dataset_id).all()
    if specs is None:
        abort(404)
    return [ row_to_dict(spec) for spec in specs ]

def insert_specs(project_id, specs):
    spec_objects = []
    for s in specs:
        spec_objects.append(Spec(
            generating_procedure = s['generating_procedure'],
            type_structure = s['type_structure'],
            viz_type = s['viz_type'],
            args = s['args'],
            meta = s['meta'],
            data = s['data'],
            score = s['score'],
            dataset_id = s['dataset_id'],
            project_id = project_id,
        ))
    db.session.add_all(spec_objects)
    db.session.commit()
    return [ row_to_dict(s) for s in spec_objects ]

def delete_spec(project_id, exported_spec_id):
    # TODO Accept multiple IDs
    try:
        spec = Spec.query.filter_by(project_id=project_id, id=exported_spec_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    db.session.delete(spec)
    db.session.commit()
    return row_to_dict(spec)

################
# Exported Specifications
################
def get_exported_specs(project_id, exported_spec):
    specs = Exported_Spec.query.filter_by(project_id=project_id).all()
    return [ row_to_dict(spec) for spec in specs ]

def insert_exported_spec(project_id, spec_id, conditional, config):
    exported_spec = Exported_Spec(
        spec_id = spec_id,
        conditional = conditional,
        config = config
    )
    db.session.add_all(exported_spec)
    db.session.commit()
    return row_to_dict(exported_spec)

def delete_exported_spec(project_id, exported_spec_id):
    exported_spec = Exported_Spec.query.filter_by(project_id=project_id, id=exported_spec_id).one()

    if exported_spec is None:
        abort(404)

    db.session.delete(exported_spec)
    db.session.commit()
    return row_to_dict(exported_spec)
