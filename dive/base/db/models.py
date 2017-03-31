import uuid
from datetime import datetime
from sqlalchemy import Table, Column, Integer, Boolean, ForeignKey, DateTime, Unicode, Enum, Float, ForeignKeyConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from dive.base.core import db
from dive.base.constants import Role, User_Status, ModelName
from dive.base.db.helpers import row_to_dict

import logging
logger = logging.getLogger(__name__)


def make_uuid():
    return unicode(uuid.uuid4())


project_preloaded_dataset_association_table = Table('project_preloaded_dataset_association',
    db.Model.metadata,
    Column('project_preloaded_dataset_id', Integer, primary_key=True),
    Column('project_id', Integer, ForeignKey('project.id')),
    Column('preloaded_dataset_id', Integer, ForeignKey('dataset.id'))
)


class CRUDBase(db.Model):
    __abstract__ = True
    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    @classmethod
    def _get_one_object(cls, **kwargs):
        try:
            obj = cls.query.filter_by(**kwargs).one()
            return obj
        except NoResultFound, e:
            logger.error(e)
            return None
        except MultipleResultsFound, e:
            logger.error(e)
            raise e    

    @classmethod
    def get_one(cls, **kwargs):
        return row_to_dict(cls._get_one_object(**kwargs))

    @classmethod
    def get_multiple(cls, **kwargs):
        objs = cls.query.filter_by(**kwargs).all()
        return [ row_to_dict(obj) for obj in objs ]

    @classmethod
    def get_count(cls, **kwargs):
        return cls.query.filter_by(**kwargs).count()

    @classmethod
    def create(cls, **kwargs):
        obj = cls(**kwargs)
        db.session.add(obj)
        db.session.commit()
        return row_to_dict(obj)

    @classmethod
    def update_by_id(cls, id, **kwargs):
        obj = cls._get_one_object(id=id)
        for k, v in kwargs.iteritems():
            setattr(obj, k, v)

        db.session.add(obj)
        db.session.commit()
        return row_to_dict(obj)        

    @classmethod
    def delete(cls, id):
        obj = cls._get_one_object(id=id)
        db.session.delete(obj)
        db.session.commit()
        return row_to_dict(obj) 


class ProjectModel(CRUDBase):
    __tablename__ = ModelName.PROJECT.value
    id = Column(Integer, primary_key=True)
    title = Column(Unicode(250))
    description = Column(Unicode(2000))
    topics = Column(JSONB)
    preloaded = Column(Boolean(), default=False)
    directory = Column(Unicode(2000))
    private = Column(Boolean())
    anonymous = Column(Boolean())
    starred = Column(Boolean(), default=False)

    user_id = Column(Integer, ForeignKey('user.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    datasets = relationship('DatasetModel',
        cascade='all, delete-orphan',
        backref='project',
        lazy='dynamic')

    preloaded_datasets = relationship('DatasetModel',
        secondary=project_preloaded_dataset_association_table,
        back_populates='projects_using',
        lazy='dynamic')

    specs = relationship('SpecModel',
        cascade='all, delete-orphan',
        backref='projects',
        lazy='dynamic')

    documents = relationship('DocumentModel',
        cascade='all, delete-orphan',
        backref='project',
        lazy='dynamic')

    correlations = relationship('CorrelationModel',
        cascade='all, delete-orphan',
        backref='project',
        lazy='dynamic')

    aggregations = relationship('AggregationModel',
        cascade='all, delete-orphan',
        backref='project',
        lazy='dynamic')

    comparisons = relationship('ComparisonModel',
        cascade='all, delete-orphan',
        backref='project',
        lazy='dynamic')

    regressions = relationship('RegressionModel',
        cascade='all, delete-orphan',
        backref='project',
        lazy='dynamic')

    @classmethod
    def get_multiple(cls, **kwargs):
        objs = cls.query.filter_by(**kwargs).all()
        for obj in objs:
            setattr(obj, 'included_datasets', [ row_to_dict(d) for d in obj.datasets ])
            setattr(obj, 'num_specs', obj.specs.count())
            setattr(obj, 'num_datasets', obj.datasets.count())
            setattr(obj, 'num_documents', obj.documents.count())
            setattr(obj, 'num_analyses', obj.aggregations.count() + obj.comparisons.count() + obj.correlations.count() + obj.regressions.count())
        return [ row_to_dict(obj, custom_fields=[ 'included_datasets', 'num_datasets', 'num_specs', 'num_documents', 'num_analyses' ]) for obj in objs ]

    @classmethod
    def get_preloaded_datasets(cls, **kwargs):
        project = cls._get_one_object(**kwargs)
        print project
        return [ row_to_dict(d) for d in project.preloaded_datasets ]

    @classmethod
    def add_preloaded_dataset_to_project(cls, project_id, dataset_id):
        project = cls._get_one_object(id=project_id)
        preloaded_dataset = Dataset._get_one_object(id=dataset_id)
        if preloaded_dataset not in project.preloaded_datasets:
            project.preloaded_datasets.append(preloaded_dataset)
            db.session.commit()
            return row_to_dict(preloaded_dataset)
        else:
            return None

class DatasetModel(CRUDBase):
    '''
    The dataset is the core entity of any access to data.
    The dataset keeps an in-memory representation of the data model
    (including all dimensions and measures) which can be used to
    generate necessary queries.
    '''
    __tablename__ = ModelName.DATASET.value
    id = Column(Integer, primary_key=True)
    title = Column(Unicode(250))
    description = Column(Unicode())
    preloaded = Column(Boolean(), default=False)

    storage_type = Column(Unicode(10))
    offset = Column(Integer)
    dialect = Column(JSONB)
    encoding = Column(Unicode(250))
    path = Column(Unicode(250))
    file_name = Column(Unicode(250))
    type = Column(Unicode(250))
    orig_type = Column(Unicode(250))
    tags = Column(JSONB)
    info_url = Column(Unicode(250))

    # One-to-one with dataset_properties
    dataset_properties = relationship('DatasetPropertiesModel',
        uselist=False,
        cascade='all, delete-orphan',
        backref='dataset')

    # One-to-many with field_properties
    fields_properties = relationship('FieldPropertiesModel',
        backref='dataset',
        cascade='all, delete-orphan',
        lazy='dynamic')

    # One-to-many with specs
    specs = relationship('SpecModel',
        backref='dataset',
        cascade='all, delete-orphan',
        lazy='dynamic')

    projects_using = relationship('ProjectModel',
        secondary=project_preloaded_dataset_association_table,
        back_populates='preloaded_datasets',
        lazy='dynamic')

    # Many-to-one with project
    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    @classmethod
    def get_one(cls, **kwargs):
        obj = cls._get_one_object(**kwargs)
        if obj and obj.preloaded or obj.project_id == kwargs.get('project_id'):
            return row_to_dict(obj)
        else:
            return None

    @classmethod
    def get_multiple(cls, include_preloaded=True, **kwargs):
        print 'Get multiple datasets', kwargs
        objs = cls.query.filter_by(**kwargs).all()

        if include_preloaded:
            project = ProjectModel._get_one_object(id=kwargs.get('project_id'))
            objs.extend(project.preloaded_datasets.all())        

        return [ row_to_dict(obj) for obj in objs ]


class DatasetPropertiesModel(CRUDBase):
    __tablename__ = ModelName.DATASET_PROPERTIES.value
    id = Column(Integer, primary_key=True)
    n_rows = Column(Integer)
    n_cols = Column(Integer)
    field_names = Column(JSONB)
    field_types = Column(JSONB)
    field_accessors = Column(JSONB)
    structure = Enum(['wide', 'long'])
    is_time_series = Column(Boolean())

    dataset_id = Column(Integer, ForeignKey('dataset.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(ProjectModel)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class FieldPropertiesModel(CRUDBase):
    __tablename__ = ModelName.FIELD_PROPERTIES.value
    id = Column(Integer, primary_key=True)
    name = Column(Unicode(250))  # Have these here, vs. in dataset_properties?
    type = Column(Unicode(250))
    scale = Column(Unicode(250))
    general_type = Column(Unicode(250))
    color = Column(Unicode(250))
    type_scores = Column(JSONB)
    index = Column(Integer)  # TODO Tie this down with a foreign key?
    normality = Column(JSONB)
    num_na = Column(Unicode(250))
    contiguous = Column(Boolean())
    is_unique = Column(Boolean())
    is_id = Column(Boolean())
    unique_values = Column(JSONB)
    child = Column(Unicode(250))
    is_child = Column(Boolean())
    viz_data = Column(JSONB)
    stats = Column(JSONB)
    manual = Column(JSONB)

    dataset_id = Column(Integer, ForeignKey('dataset.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    project = relationship(ProjectModel)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


# TODO Make this not dataset-specific?
class SpecModel(CRUDBase):
    '''
    Many-to-one with Dataset
    '''
    __tablename__ = ModelName.SPEC.value
    id = Column(Integer, primary_key=True)
    case = Column(Unicode(250))
    generating_procedure = Column(Unicode(250))
    type_structure = Column(Unicode(250))
    recommendation_type = Column(Unicode(20))
    recommendation_types = Column(JSONB)
    viz_types = Column(JSONB)
    args = Column(JSONB)
    meta = Column(JSONB)
    scores = Column(JSONB)
    data = Column(JSONB)
    field_ids = Column(JSONB)
    selected_fields = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    # One-to-many with exported specs
    exported_specs = relationship('ExportedSpecModel',
        backref='spec',
        cascade='all, delete-orphan',
        lazy='dynamic')

    dataset_id = Column(Integer, ForeignKey('dataset.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

class ExportedSpecModel(CRUDBase):
    '''
    Many-to-one with Specification
    '''
    __tablename__ = ModelName.EXPORTED_SPEC.value
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    spec_id = Column(Integer, ForeignKey('spec.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(ProjectModel)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class DocumentModel(CRUDBase):
    __tablename__ = ModelName.DOCUMENT.value
    id = Column(Integer, primary_key=True)
    title = Column(Unicode(250))
    content = Column(JSONB)

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)




class RegressionModel(CRUDBase):
    '''
    Many-to-one with Dataset
    '''
    __tablename__ = ModelName.REGRESSION.value
    id = Column(Integer, primary_key=True)

    spec = Column(JSONB)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    # One-to-many with exported specs
    exported_regression = relationship('ExportedRegressionModel',
        backref='regression',
        cascade='all, delete-orphan',
        lazy='dynamic')

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class ExportedRegressionModel(CRUDBase):
    __tablename__ = ModelName.EXPORTED_REGRESSION.value
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    regression_id = Column(Integer, ForeignKey('regression.id',
        onupdate='CASCADE', ondelete='CASCADE'))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(ProjectModel)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

class InteractionTermModel(CRUDBase):
    __tablename__ = ModelName.INTERACTION_TERM.value
    id = Column(Integer, primary_key=True)
    variables = Column(JSONB)
    names = Column(JSONB)

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(ProjectModel)

    dataset_id = Column(Integer, ForeignKey('dataset.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

class AggregationModel(CRUDBase):
    __tablename__ = ModelName.AGGREGATION.value
    id = Column(Integer, primary_key=True)

    spec = Column(JSONB)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    # One-to-many with exported specs
    exported_aggregation = relationship('ExportedAggregationModel',
        backref='aggregation',
        cascade='all, delete-orphan',
        lazy='dynamic')

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class ExportedAggregationModel(CRUDBase):
    __tablename__ = ModelName.EXPORTED_AGGREGATION.value
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    aggregation_id = Column(Integer, ForeignKey('aggregation.id',
        onupdate='CASCADE', ondelete='CASCADE'))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(ProjectModel)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class ComparisonModel(CRUDBase):
    __tablename__ = ModelName.COMPARISON.value
    id = Column(Integer, primary_key=True)

    spec = Column(JSONB)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    # One-to-many with exported specs
    exported_comparison = relationship('ExportedComparisonModel',
        backref='comparison',
        cascade='all, delete-orphan',
        lazy='dynamic')

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class ExportedComparisonModel(CRUDBase):
    __tablename__ = ModelName.EXPORTED_COMPARISON.value
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    comparison_id = Column(Integer, ForeignKey('comparison.id',
        onupdate='CASCADE', ondelete='CASCADE'))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(ProjectModel)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class CorrelationModel(CRUDBase):
    __tablename__ = ModelName.CORRELATION.value
    id = Column(Integer, primary_key=True)

    spec = Column(JSONB)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    # One-to-many with exported specs
    exported_correlation = relationship('ExportedCorrelationModel',
        backref='correlation',
        cascade='all, delete-orphan',
        lazy='dynamic')

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class ExportedCorrelationModel(CRUDBase):
    __tablename__ = ModelName.EXPORTED_CORRELATION.value
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    correlation_id = Column(Integer, ForeignKey('correlation.id',
        onupdate='CASCADE', ondelete='CASCADE'))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(ProjectModel)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class FeedbackModel(CRUDBase):
    __tablename__ = ModelName.FEEDBACK.value
    id = Column(Integer, primary_key=True)

    feedback_type = Column(Unicode(250))
    description = Column(Unicode(2000))

    user_id = Column(Integer, ForeignKey('user.id',
        onupdate='CASCADE', ondelete='CASCADE'))

    user_email = Column(Unicode(50))
    user_username = Column(Unicode(50))
    path = Column(Unicode(250))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    project = relationship(ProjectModel)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class RelationshipModel(CRUDBase):
    '''
    Relationships between fields in different datasets
    '''
    __tablename__ = ModelName.RELATIONSHIP.value
    id = Column(Integer, primary_key=True)

    source_dataset_id = Column(Integer, ForeignKey('dataset.id'),)
    source_field_id = Column(Integer, ForeignKey('field_properties.id'))
    target_dataset_id = Column(Integer, ForeignKey('dataset.id'))
    target_field_id = Column(Integer, ForeignKey('field_properties.id'))

    source_dataset_name = Column(Unicode(250))
    source_field_name = Column(Unicode(250))
    target_dataset_name = Column(Unicode(250))
    target_field_name = Column(Unicode(250))

    distance = Column(Float)
    type = Column(Unicode(250))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'))
    project = relationship(ProjectModel)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


team_user_association_table = Table('team_user_association',
    db.Model.metadata,
    Column('team_id', Integer, ForeignKey('team.id')),
    Column('user_id', Integer, ForeignKey('user.id'))
)

team_admin_association_table = Table('team_admin_association',
    db.Model.metadata,
    Column('team_id', Integer, ForeignKey('team.id')),
    Column('admin_id', Integer, ForeignKey('user.id'))
)

class TeamModel(CRUDBase):
    '''
    Many-to-many with User
    '''
    __tablename__ = ModelName.TEAM.value
    id = Column(Integer, primary_key=True)
    name = Column(Unicode(50), unique=True)
    users = relationship('UserModel', secondary=team_user_association_table, back_populates="teams", lazy='dynamic')
    admins = relationship('UserModel', secondary=team_admin_association_table, back_populates="admin", lazy='dynamic')

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class UserModel(CRUDBase):
    '''
    Many-to-one with Group
    '''
    __tablename__ = ModelName.USER.value
    id = Column(Integer, primary_key=True)
    username = Column(Unicode(50), unique=True)
    email = Column(Unicode(120))
    password = Column(Unicode(120))

    authenticated = Column(Boolean(), default=True)
    anonymous = Column(Boolean(), default=False)
    active = Column(Boolean(), default=True)
    confirmed = Column(Boolean(), default=True)
    confirmed_on = Column(DateTime, nullable=True)

    api_key = Column(Unicode(2000), default=make_uuid)

    admin = relationship('TeamModel', secondary=team_admin_association_table, back_populates="admins", lazy='dynamic')
    teams = relationship('TeamModel', secondary=team_user_association_table, back_populates="users", lazy='dynamic')

    status = Column(Unicode(20), default=User_Status.NEW.value)

    projects = relationship('ProjectModel',
        backref='user',
        cascade='all, delete-orphan',
        lazy='dynamic'
    )

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    def __init__(self, username='', name='', email='', password='', role='', confirmed=False, anonymous=False):
        self.api_key = make_uuid()
        self.username = username
        self.email = email
        self.password = password
        self.role = role
        self.confirmed = confirmed
        self.anonymous = anonymous

    def is_authenticated(self):
        return self.authenticated

    def is_anonymous(self):
        return self.anonymous

    def is_global_admin(self):
        return (self.admin.filter_by(name='global').count() > 0)

    def is_active(self):
        return self.active

    def get_id(self):
        return unicode(self.id)

    @classmethod
    def confirm_user(cls, **kwargs):
        user = cls._get_one_object(**kwargs)
        user.confirmed = True
        user.confirmed_on = datetime.datetime.now()
        db.session.commit()
        return user