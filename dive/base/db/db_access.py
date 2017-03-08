'''
Module containing functions and Data Access Objects for accessing the database.
Parameters in, JSONable objects out.

Mainly used to separate session management from models, and to provide uniform
db interfaces to both the API and compute layers.
'''

from flask import abort
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from dive.base.core import db
from dive.base.db import ModelName, row_to_dict
from dive.base.db.models import Project, Dataset, Dataset_Properties, Field_Properties, \
    Spec, Exported_Spec, Regression, Exported_Regression, Interaction_Term, Team, User, \
    Relationship, Document, Aggregation, Exported_Aggregation, Correlation, Exported_Correlation, Feedback
from dive.server.resources import ContentType

import logging
logger = logging.getLogger(__name__)


################
# Projects
# https://github.com/sloria/PythonORMSleepy/blob/master/sleepy/api_sqlalchemy.py
################
def get_project(project_id):
    project = Project.query.get_or_404(int(project_id))
    return row_to_dict(project)

def get_projects(**kwargs):
    projects = Project.query.filter_by(**kwargs).all()
    for project in projects:
        setattr(project, 'included_datasets', [ row_to_dict(d) for d in project.datasets ])
        setattr(project, 'num_specs', project.specs.count())
        setattr(project, 'num_datasets', project.datasets.count())
        setattr(project, 'num_documents', project.documents.count())
        setattr(project, 'num_analyses', project.aggregations.count() + project.comparisons.count() + project.correlations.count() + project.regressions.count())
    return [ row_to_dict(project, custom_fields=[ 'included_datasets', 'num_datasets', 'num_specs', 'num_documents', 'num_analyses' ]) for project in projects ]

def insert_project(**kwargs):
    project = Project(
        **kwargs
    )
    db.session.add(project)
    db.session.commit()
    return row_to_dict(project)

def update_project(project_id, **kwargs):
    project = Project.query.get_or_404(int(project_id))

    for k, v in kwargs.iteritems():
        setattr(project, k, v)

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
    try:
        dataset = Dataset.query.filter_by(id=dataset_id).one()
        if dataset.preloaded or dataset.project_id == project_id:
            return row_to_dict(dataset)
        else:
            return None

    # TODO Decide between raising error and aborting with 404
    except NoResultFound, e:
        logger.error(e)
        return None

    except MultipleResultsFound, e:
        logger.error(e)
        raise e

def get_datasets(project_id, include_preloaded=True, **kwargs):
    datasets = Dataset.query.filter_by(project_id=project_id, **kwargs).all()

    if include_preloaded:
        project = Project.query.get_or_404(project_id)
        datasets.extend(project.preloaded_datasets.all())
    return [ row_to_dict(dataset) for dataset in datasets ]

def insert_dataset(project_id, **kwargs):
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
# Preloaded Datasets
################
def get_preloaded_datasets(**kwargs):
    datasets = Dataset.query.filter_by(preloaded=True).all()
    return [ row_to_dict(dataset) for dataset in datasets ]

def insert_preloaded_dataset(**kwargs):
    dataset = Dataset(
        **kwargs
    )
    db.session.add(dataset)
    db.session.commit()
    return row_to_dict(dataset)

def add_preloaded_dataset_to_project(project_id, dataset_id):
    try:
        project = Project.query.filter_by(id=project_id).one()
        preloaded_dataset = Dataset.query.filter_by(id=dataset_id, preloaded=True).one()
        if preloaded_dataset not in project.preloaded_datasets:
            project.preloaded_datasets.append(preloaded_dataset)
            db.session.commit()
            return row_to_dict(preloaded_dataset)
        else:
            return None
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

def remove_preloaded_dataset_from_project(project_id, dataset_id):
    try:
        project = Project.query.filter_by(id=project_id).one()
        preloaded_dataset = Dataset.query.filter_by(id=dataset_id, preloaded=True).one()
        if preloaded_dataset in project.preloaded_datasets:
            project.preloaded_datasets.remove(preloaded_dataset)
            db.session.commit()
            return row_to_dict(preloaded_dataset)
        else:
            return None
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

def get_project_preloaded_datasets(project_id):
    project = Project.query.get_or_404(int(project_id))
    return [ row_to_dict(d) for d in project.preloaded_datasets ]

################
# Dataset Properties
################
def get_dataset_properties(project_id, dataset_id):
    try:
        dataset_properties = Dataset_Properties.query.filter_by(dataset_id=dataset_id).one()
        if dataset_properties.dataset.preloaded or dataset_properties.project_id == project_id:
            return row_to_dict(dataset_properties)
        else:
            return None
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

# TODO Do an upsert?
def insert_dataset_properties(project_id, dataset_id, **kwargs):
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
        setattr(dataset_properties, k, v)

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
    result = Field_Properties.query.filter_by(dataset_id=dataset_id, **kwargs).all()
    field_properties = [ row_to_dict(r) for r in result if (r.dataset.preloaded or r.project_id == project_id) ]
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
        setattr(field_properties, k, v)

    db.session.commit()
    return row_to_dict(field_properties)


def update_field_properties_type_by_id(project_id, field_id, field_type, general_type):
    field_properties = Field_Properties.query.filter_by(
        id=field_id,
        project_id=project_id,
        ).one()

    field_properties.type = field_type
    field_properties.general_type = general_type
    field_properties.manual.update({
        'type': True
    })

    db.session.commit()
    return row_to_dict(field_properties)

def update_field_properties_is_id_by_id(project_id, field_id, field_is_id):
    field_properties = Field_Properties.query.filter_by(
        id=field_id,
        project_id=project_id,
        ).one()

    field_properties.is_id = field_is_id
    field_properties.manual.update({
        'is_id': True
    })

    db.session.commit()
    return row_to_dict(field_properties)

def update_field_properties_color_by_id(project_id, field_id, field_color):
    field_properties = Field_Properties.query.filter_by(
        id=field_id,
        project_id=project_id,
        ).one()

    field_properties.color = field_color
    field_properties.manual.update({
        'color': True
    })

    db.session.commit()
    return row_to_dict(field_properties)


def get_variable_names_by_id(id_list):
    name_list = []
    for variable_id in id_list:
        name = Field_Properties.query.filter_by(id=variable_id).one().name
        name_list.append(name)
    return name_list

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
    spec = Spec.query.filter_by(id=spec_id, **kwargs).one()
    if spec is None:
        abort(404)
    exported_spec_ids = [ es.id for es in spec.exported_specs.all() ]
    if exported_spec_ids:
        exported = True
    else:
        exported = False
    setattr(spec, 'exported', exported)
    setattr(spec, 'exported_spec_ids', exported_spec_ids)
    return row_to_dict(spec, custom_fields=[ 'exported', 'exported_spec_ids'])

def get_specs(project_id, dataset_id, **kwargs):
    specs = Spec.query.filter_by(dataset_id=dataset_id, **kwargs).all()
    specs = [ s for s in specs if (s.dataset.preloaded or s.project_id == project_id)]
    if specs is None:
        abort(404)
    final_specs = []
    for spec in specs:
        exported_spec_ids = [ es.id for es in spec.exported_specs.all() ]
        if exported_spec_ids:
            exported = True
        else:
            exported = False
        setattr(spec, 'exported', exported)
        setattr(spec, 'exported_spec_ids', exported_spec_ids)
    return [ row_to_dict(s, custom_fields=[ 'exported', 'exported_spec_ids' ]) for s in specs ]


from time import time
def insert_specs(project_id, specs, selected_fields, recommendation_types, conditionals, config):
    start_time = time()
    spec_objects = []
    for s in specs:
        spec_object = Spec(
            project_id = project_id,
            selected_fields = selected_fields,
            conditionals = conditionals,
            recommendation_types = recommendation_types,
            config = config,
            **s
        )
        setattr(spec_object, 'exported', False)
        setattr(spec_object, 'exported_spec_ids', [])
        spec_objects.append(spec_object)

    db.session.add_all(spec_objects)
    db.session.commit()
    logger.info('Insertion took %s seconds', (time() - start_time))
    return [ row_to_dict(s, custom_fields=[ 'exported', 'exported_spec_ids' ]) for s in spec_objects ]

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
def get_public_exported_spec(exported_spec_id, spec_type):
    try:
        if spec_type == ContentType.VISUALIZATION.value:
            exported_spec = Exported_Spec.query.filter_by(
                id=exported_spec_id
            ).one()
            desired_spec_keys = [ 'generating_procedure', 'type_structure', 'viz_types', 'meta', 'args', 'dataset_id' ]
            for desired_spec_key in desired_spec_keys:
                value = getattr(exported_spec.spec, desired_spec_key)
                setattr(exported_spec, desired_spec_key, value)
            return row_to_dict(exported_spec, custom_fields=desired_spec_keys)

        elif spec_type == ContentType.CORRELATION.value:
            exported_spec = Exported_Correlation.query.filter_by(
                id=exported_spec_id
            ).one()
            return row_to_dict(exported_spec)

        elif spec_type == ContentType.REGRESSION.value:
            exported_spec = Exported_Regression.query.filter_by(
                id=exported_spec_id
            ).one()
            setattr(exported_spec, 'spec', exported_spec.regression.spec)
            setattr(exported_spec, 'type', 'regression')
            return row_to_dict(exported_spec, custom_fields=['type', 'spec'])
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

def get_exported_spec(project_id, exported_spec_id):
    try:
        spec = Exported_Spec.query.filter_by(
            id=exported_spec_id,
            project_id=project_id
        ).one()
        return row_to_dict(spec)
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

def get_exported_spec_by_fields(project_id, spec_id, **kwargs):
    try:
        spec = Exported_Spec.query.filter_by(
            spec_id = spec_id,
            project_id = project_id,
            **kwargs
        ).one()
        return row_to_dict(spec)
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

def get_exported_specs(project_id):
    exported_specs = Exported_Spec.\
        query.\
        filter_by(project_id=project_id).\
        all()

    desired_spec_keys = [ 'args', 'generating_procedure', 'type_structure', 'viz_types', 'meta', 'dataset_id' ]

    final_specs = []
    for exported_spec in exported_specs:
        final_spec = exported_spec
        for desired_spec_key in desired_spec_keys:
            value = getattr(final_spec.spec, desired_spec_key)
            setattr(final_spec, desired_spec_key, value)
        final_specs.append(final_spec)
    return [ row_to_dict(final_spec, custom_fields=desired_spec_keys) for final_spec in final_specs ]

def insert_exported_spec(project_id, spec_id, data, conditionals, config):
    exported_spec = Exported_Spec(
        project_id = project_id,
        spec_id = spec_id,
        data = data,
        conditionals = conditionals,
        config = config
    )

    db.session.add(exported_spec)
    db.session.commit()

    spec = Spec.query.filter_by(id=spec_id, project_id=project_id).one()
    if spec is None:
        abort(404)

    desired_spec_keys = [ 'args', 'generating_procedure', 'type_structure', 'viz_types', 'meta', 'dataset_id' ]
    for desired_spec_key in desired_spec_keys:
        value = getattr(spec, desired_spec_key)
        setattr(exported_spec, desired_spec_key, value)

    return row_to_dict(exported_spec, custom_fields=desired_spec_keys)

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
def get_regression_by_id(regression_id, project_id, **kwargs):
    regression = Regression.query.filter_by(id=regression_id, project_id=project_id, **kwargs).one()
    if regression is None:
        abort(404)
    return row_to_dict(regression)


def get_regression_from_spec(project_id, spec, **kwargs):
    try:
        regression = Regression.query.filter_by(project_id=project_id, spec=spec, **kwargs).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        logger.error(e)
        return row_to_dict(Regression.query.filter_by(project_id=project_id, spec=spec, **kwargs).all()[0])
    return row_to_dict(regression)


def insert_regression(project_id, spec, data, **kwargs):
    regression = Regression(
        project_id = project_id,
        spec = spec,
        data = data,
        **kwargs
    )
    db.session.add(regression)
    db.session.commit()
    return row_to_dict(regression)

def delete_regression(project_id, regression_id, **kwargs):
    try:
        regression = Regression.query.filter_by(project_id=project_id, id=regression_id, **kwargs).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    db.session.delete(regression)
    db.session.commit()
    return row_to_dict(regression)

def get_correlation_by_id(correlation_id, project_id, **kwargs):
    correlation = Correlation.query.filter_by(id=correlation_id, project_id=project_id, **kwargs).one()
    if correlation is None:
        abort(404)
    return row_to_dict(correlation)


def get_correlation_from_spec(project_id, spec, **kwargs):
    try:
        correlation = Correlation.query.filter_by(project_id=project_id, spec=spec, **kwargs).one()
    except NoResultFound:
        return None
    return row_to_dict(correlation)


def insert_correlation(project_id, spec, data, **kwargs):
    correlation = Correlation(
        project_id = project_id,
        spec = spec,
        data = data,
        **kwargs
    )
    db.session.add(correlation)
    db.session.commit()
    return row_to_dict(correlation)

def delete_correlation(project_id, correlation_id, **kwargs):
    try:
        correlation = Correlation.query.filter_by(project_id=project_id, id=correlation_id, **kwargs).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    db.session.delete(correlation)
    db.session.commit()
    return row_to_dict(correlation)

################
# Summaries
################
def get_aggregation_by_id(aggregation_id, project_id, **kwargs):
    aggregation = Aggregation.query.filter_by(id=aggregation_id, project_id=project_id, **kwargs).one()
    if aggregation is None:
        abort(404)
    return row_to_dict(aggregation)


def get_aggregation_from_spec(project_id, spec, **kwargs):
    try:
        aggregation = Aggregation.query.filter_by(project_id=project_id, spec=spec, **kwargs).one()
    except NoResultFound:
        return None
    return row_to_dict(aggregation)


def insert_aggregation(project_id, spec, data, **kwargs):
    aggregation = Aggregation(
        project_id = project_id,
        spec = spec,
        data = data,
        **kwargs
    )
    db.session.add(aggregation)
    db.session.commit()
    return row_to_dict(aggregation)

def delete_aggregation(project_id, aggregation_id, **kwargs):
    try:
        aggregation = Aggregation.query.filter_by(project_id=project_id, id=aggregation_id, **kwargs).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    db.session.delete(aggregation)
    db.session.commit()
    return row_to_dict(aggregation)

################
# Exported Analyses
################

# Regressions
def get_exported_regression_by_id(project_id, exported_regression_id):
    try:
        exported_regression = Exported_Regression.query.filter_by(id=exported_regression_id,
            project_id=project_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    return row_to_dict(exported_regression)

def get_exported_regression_by_regression_id(project_id, regression_id):
    try:
        exported_regression = Exported_Regression.query.filter_by(regression_id=regression_id,
            project_id=project_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    return row_to_dict(exported_regression)

def get_exported_regressions(project_id):
    exported_regressions = Exported_Regression.query.filter_by(project_id=project_id).all()
    for e in exported_regressions:
        setattr(e, 'spec', e.regression.spec)
        setattr(e, 'type', 'regression')
    return [ row_to_dict(exported_regression, custom_fields=['type', 'spec']) for exported_regression in exported_regressions ]

def insert_exported_regression(project_id, regression_id, data, conditionals, config):
    exported_regression = Exported_Regression(
        project_id = project_id,
        regression_id = regression_id,
        data = data,
        conditionals = conditionals,
        config = config
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

###################
# Interaction Terms
###################

def insert_interaction_term(project_id, dataset_id, variables):
    names = get_variable_names_by_id(variables)
    interaction_term = Interaction_Term(
        project_id=project_id,
        dataset_id=dataset_id,
        variables=variables,
        names=names
    )
    db.session.add(interaction_term)
    db.session.commit()
    return row_to_dict(interaction_term)

def get_interaction_terms(project_id, dataset_id):
    result = Interaction_Term.query.filter_by(project_id=project_id, dataset_id=dataset_id).all()
    interaction_terms = [ row_to_dict(r) for r in result ]
    return interaction_terms

def get_interaction_term_properties(interaction_term_ids):
    properties_list = []
    for interaction_term_id in interaction_term_ids:
        term_properties = []
        variable_ids = Interaction_Term.query.filter_by(id=interaction_term_id).one().variables
        for variable_id in variable_ids:
            data = Field_Properties.query.filter_by(id=variable_id).one()
            term_properties.append(row_to_dict(data))
        properties_list.append(term_properties)
    return properties_list

def delete_interaction_term(interaction_term_id):
    try:
        interaction_term = Interaction_Term.query.filter_by(id=interaction_term_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

    db.session.delete(interaction_term)
    db.session.commit()
    return row_to_dict(interaction_term)


##############
# Correlations
##############

def get_exported_correlation_by_id(project_id, exported_correlation_id):
    try:
        exported_correlation = Exported_Correlation.query.filter_by(id=exported_correlation_id,
            project_id=project_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    return row_to_dict(exported_correlation)

def get_exported_correlation_by_correlation_id(project_id, correlation_id):
    try:
        exported_correlation = Exported_Correlation.query.filter_by(correlation_id=correlation_id,
            project_id=project_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    return row_to_dict(exported_correlation)

def get_exported_correlations(project_id):
    exported_correlations = Exported_Correlation.query.filter_by(project_id=project_id).all()
    for e in exported_correlations:
        setattr(e, 'spec', e.correlation.spec)
        setattr(e, 'type', 'correlation')
    return [ row_to_dict(exported_correlation, custom_fields=['type', 'spec']) for exported_correlation in exported_correlations ]

def insert_exported_correlation(project_id, correlation_id, data, conditionals, config):
    exported_correlation = Exported_Correlation(
        project_id = project_id,
        correlation_id = correlation_id,
        data = data,
        conditionals = conditionals,
        config = config
    )
    db.session.add(exported_correlation)
    db.session.commit()
    return row_to_dict(exported_correlation)

def delete_exported_correlation(project_id, exported_correlation_id):
    try:
        exported_correlation = Exported_Correlation.query.filter_by(project_id=project_id, id=exported_correlation_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

    db.session.delete(exported_correlation)
    db.session.commit()
    return row_to_dict(exported_correlation)


# Aggregation
def get_exported_aggregation_by_id(project_id, exported_aggregation_id):
    exported_aggregation = Exported_Aggregation.query.filter_by(id=exported_aggregation_id,
        project_id=project_id).one()
    if exported_aggregation is None:
        abort(404)
    return row_to_dict(exported_aggregation)

def get_exported_aggregations(project_id):
    exported_aggregations = Exported_Aggregation.query.filter_by(project_id=project_id).all()
    for e in exported_aggregations:
        setattr(e, 'spec', e.aggregation.spec)
        setattr(e, 'type', 'aggregation')
    return [ row_to_dict(exported_aggregation, custom_fields=['type', 'spec']) for exported_aggregation in exported_aggregations ]

def insert_exported_aggregation(project_id, aggregation_id, data, conditionals, config):
    exported_aggregation = Exported_Aggregation(
        project_id = project_id,
        aggregation_id = aggregation_id,
        data = data,
        conditionals = conditionals,
        config = config
    )
    db.session.add(exported_aggregation)
    db.session.commit()
    return row_to_dict(exported_aggregation)

def delete_exported_aggregation(project_id, exported_aggregation_id):
    try:
        exported_aggregation = Exported_Aggregation.query.filter_by(project_id=project_id, id=exported_aggregation_id).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e

    db.session.delete(exported_aggregation)
    db.session.commit()
    return row_to_dict(exported_aggregation)

################
# Documents
################
def get_documents(project_id):
    documents = Document.query.filter_by(project_id=project_id).all()
    if documents is None:
        abort(404)
    return [ row_to_dict(d) for d in documents ]

def get_public_document(document_id):
    try:
        document = Document.query.filter_by(id=document_id).one()
        return row_to_dict(document)
    except NoResultFound, e:
        logger.error(e)
        return None
    except MultipleResultsFound, e:
        logger.error(e)
        raise e

def get_document(project_id, document_id):
    try:
        document = Document.query.filter_by(project_id=project_id, id=document_id).one()
        return row_to_dict(document)
    except NoResultFound, e:
        logger.error(e)
        return None
    except MultipleResultsFound, e:
        logger.error(e)
        raise e

def create_document(project_id, title='Unnamed Document', content={ 'blocks': [] }):
    document = Document(
        project_id=project_id,
        title=title,
        content=content
    )
    db.session.add(document)
    db.session.commit()
    return row_to_dict(document)

def update_document(project_id, document_id, title, content):
    document = Document.query.filter_by(project_id=project_id, id=document_id).one()
    document.content = content
    document.title = title
    db.session.add(document)
    db.session.commit()
    return row_to_dict(document)

def delete_document(project_id, document_id):
    document = Document.query.filter_by(project_id=project_id, id=document_id).one()
    db.session.delete(document)
    db.session.commit()
    return row_to_dict(document)


################
# Feedback
################
def submit_feedback(project_id, user_id, user_email, user_username, feedback_type, description, path):
    feedback = Feedback(
        project_id=project_id,
        user_id=user_id,
        user_email=user_email,
        user_username=user_username,
        feedback_type=feedback_type,
        description=description,
        path=path
    )
    db.session.add(feedback)
    db.session.commit()
    return row_to_dict(feedback)
