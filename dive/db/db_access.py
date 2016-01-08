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
    Spec, Exported_Spec, Regression, Exported_Regression, Group, User, Relationship, Document


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


def update_field_properties_type_by_id(project_id, field_id, field_type, general_type):
    field_properties = Field_Properties.query.filter_by(
        id=field_id,
        project_id=project_id,
        ).one()

    field_properties.type = field_type
    field_properties.general_type = general_type
    field_properties.manual = True

    db.session.commit()
    return row_to_dict(field_properties)


################
# Relationships
################
def insert_relationships(relationships, project_id):
    relationship_objects = []
    for r in relationships:
        relationship_objects.append(Relationship(
            project_id = project_id,
            **r
        ))
    db.session.add_all(relationship_objects)
    db.session.commit()
    return [ row_to_dict(r) for r in relationship_objects ]


################
# Specifications
################
def get_spec(spec_id, project_id, **kwargs):
    spec = Spec.query.filter_by(id=spec_id, project_id=project_id, **kwargs).one()
    if spec is None:
        abort(404)
    return row_to_dict(spec)

def get_specs(project_id, dataset_id, **kwargs):
    specs = Spec.query.filter_by(project_id=project_id, dataset_id=dataset_id, **kwargs).all()
    if specs is None:
        abort(404)
    return [ row_to_dict(spec) for spec in specs ]

def insert_specs(project_id, specs, selected_fields, conditionals):
    spec_objects = []
    for s in specs:
        spec_objects.append(Spec(
            project_id = project_id,
            selected_fields = selected_fields,
            conditionals = conditionals,
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
def get_exported_spec(project_id, exported_spec_id):
    spec = Exported_Spec.query.filter_by(id=exported_spec_id,
        project_id=project_id).one()
    if spec is None:
        abort(404)
    return row_to_dict(spec)

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


################
# Analyses
################
def get_regression_by_id(regression_id, project_id):
    regression = Regression.query.filter_by(id=regression_id, project_id=project_id).one()
    if regression is None:
        abort(404)
    return row_to_dict(regression)


def get_regression_from_spec(project_id, spec):
    try:
        regression = Regression.query.filter_by(project_id=project_id, spec=spec).one()
    except NoResultFound:
        return None
    return row_to_dict(regression)


def insert_regression(project_id, spec, data):
    regression = Regression(
        project_id = project_id,
        spec = spec,
        data = data
    )
    db.session.add(regression)
    db.session.commit()
    return row_to_dict(regression)

def delete_regression(project_id, regression_id):
    try:
        regression = Regression.query.filter_by(project_id=project_id, id=regression_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    db.session.delete(regression)
    db.session.commit()
    return row_to_dict(regression)

################
# Exported Analyses
################
def get_exported_regression_by_id(project_id, exported_regression_id):
    exported_regression = Exported_Regression.query.filter_by(id=exported_regression_id,
        project_id=project_id).one()
    if exported_regression is None:
        abort(404)
    return row_to_dict(exported_regression)

def get_exported_regressions(project_id):
    exported_regressions = Exported_Regression.query.filter_by(project_id=project_id).all()
    return [ row_to_dict(exported_regression) for exported_regression in exported_regressions ]

def insert_exported_regression(project_id, regression_id):
    exported_regression = Exported_Regression(
        project_id = project_id,
        regression_id = regression_id
    )
    db.session.add(exported_regression)
    db.session.commit()
    return row_to_dict(exported_regression)

def delete_exported_regression(project_id, exported_regression_id):
    try:
        exported_regression = Exported_Regression.query.filter_by(project_id=project_id, id=exported_regression_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

    db.session.delete(exported_regression)
    db.session.commit()
    return row_to_dict(exported_regression)


################
# Documents
################
def get_document(project_id, document_id):
    logger.info('In get_document')
    try:
        document = Document.query.filter_by(project_id=project_id, id=document_id).one()
        return row_to_dict(document)
    except NoResultFound, e:
        logger.error(e)
        return None
    except MultipleResultsFound, e:
        logger.error(e)
        raise e

def create_document(project_id, content):
    document = Document(
        project_id=project_id,
        content=content
    )
    db.session.add(document)
    db.session.commit()
    return row_to_dict(document)

def update_document(project_id, document_id, content):
    document = Document.query.filter_by(project_id=project_id, id=document_id).one()
    document.content = content
    db.session.add(document)
    db.session.commit()
    return row_to_dict(document)

def delete_document(project_id, document_id):
    document = Document.query.filter_by(project_id=project_id, id=document_id).one()
    db.session.delete(document)
    db.session.commit()
    return row_to_dict(document)
