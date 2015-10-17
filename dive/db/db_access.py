'''
Module containing functions and Data Access Objects for accessing the database.
Parameters in, JSONable objects out.

Mainly used to separate session management from models, and to provide uniform
db interfaces to both the API and compute layers.
'''

from flask import abort
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound

from dive.core import db
from dive.db import ModelName
from dive.db.models import Project, Dataset, Dataset_Properties, Field_Properties, \
    Spec, Exported_Spec, Group, User


import logging
logger = logging.getLogger(__name__)


def row_to_dict(r):
    return { c.name: getattr(r, c.name) for c in r.__table__.columns }


model_from_name = {
    ModelName.PROJECT.value: Project,
    ModelName.DATASET.value: Dataset,
    ModelName.DATASET_PROPERTIES.value: Dataset_Properties,
    ModelName.FIELD_PROPERTIES.value: Field_Properties,
    ModelName.SPEC.value: Spec,
    ModelName.EXPORTED_SPEC.value: Exported_Spec,
    ModelName.GROUP.value: Group,
    ModelName.USER.value: User,
}

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
    project = Project(**kwargs)
    db.session.add(project)
    db.session.commit()
    return row_to_dict(project)

def update_project(project_id, **kwargs):
    title = kwargs.get('title')
    description = kwargs.get('description')

    project = Project.query.get_or_404(int(project_id))

    for k, v in kwargs.iteritems():
        project[k] = v

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
    logger.debug("Get dataset with project_id %s and dataset_id %s", project_id, dataset_id)
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
    logger.debug("Insert dataset with project_id %s", project_id)

    dataset = Dataset(
        project_id=project_id,
        **kwargs
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
        return None
    except MultipleResultsFound, e:
        raise e

# TODO Do an upsert?
def insert_dataset_properties(project_id, dataset_id, **kwargs):
    logger.debug("Insert data properties with project_id %s, and dataset_id %s", project_id, dataset_id)
    dataset_properties = Dataset_Properties(
        dataset_id = dataset_id,
        project_id = project_id,
        **kwargs
    )
    db.session.add(dataset_properties)
    db.session.commit()
    return row_to_dict(dataset_properties)

def update_dataset_properties(project_id, dataset_id, **kwargs):

    dataset_properties = Dataset_Properties.query.filter_by(project_id=project_id,
        dataset_id=dataset_id,
        ).one()

    for k, v in kwargs.iteritems():
        dataset_properties[k] = v

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
    result = Field_Properties.query.filter_by(project_id=project_id, dataset_id=dataset_id, **kwargs).all()
    field_properties = [ row_to_dict(r) for r in result ]
    return field_properties


def insert_field_properties(project_id, dataset_id, **kwargs):
    field_properties = Field_Properties(
        dataset_id = dataset_id,
        project_id = project_id,
        **kwargs
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

    for k, v in kwargs.iteritems():
        field_properties[k] = v

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
    # TODO filter by JSON rows?
    specs = Spec.query.filter_by(project_id=project_id, dataset_id=dataset_id).all()
    if specs is None:
        abort(404)
    return [ row_to_dict(spec) for spec in specs ]

def insert_specs(project_id, specs):
    spec_objects = []
    for s in specs:
        spec_objects.append(Spec(
            project_id = project_id,
            **s
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
def get_exported_specs(project_id):
    specs = Exported_Spec.query.filter_by(project_id=project_id).all()
    return [ row_to_dict(spec) for spec in specs ]

def insert_exported_spec(project_id, spec_id, conditionals, config):
    exported_spec = Exported_Spec(
        project_id = project_id,
        spec_id = spec_id,
        conditionals = conditionals,
        config = config
    )
    db.session.add(exported_spec)
    db.session.commit()
    return row_to_dict(exported_spec)

def delete_exported_spec(project_id, exported_spec_id):
    exported_spec = Exported_Spec.query.filter_by(project_id=project_id, id=exported_spec_id).one()

    if exported_spec is None:
        abort(404)

    db.session.delete(exported_spec)
    db.session.commit()
    return row_to_dict(exported_spec)
