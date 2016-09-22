import uuid
from datetime import datetime
from flask.ext.user import UserMixin
from sqlalchemy import Table, Column, Integer, Boolean, ForeignKey, DateTime, Unicode, Enum, Float
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from constants import Role, User_Status

from dive.base.core import db
from dive.base.db import ModelName

def make_uuid():
    return unicode(uuid.uuid4())

class Project(db.Model):
    __tablename__ = ModelName.PROJECT.value
    id = Column(Integer, primary_key=True)
    title = Column(Unicode(250))
    description = Column(Unicode(2000))
    topics = Column(JSONB)
    preloaded = Column(Boolean())
    directory = Column(Unicode(2000))
    private = Column(Boolean())
    anonymous = Column(Boolean())

    user_id = Column(Integer, ForeignKey('user.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    datasets = relationship('Dataset',
        cascade='all, delete-orphan',
        backref='project',
        lazy='dynamic')

    specs = relationship('Spec',
        cascade='all, delete-orphan',
        backref='project',
        lazy='dynamic')

    documents = relationship('Document',
        cascade='all, delete-orphan',
        backref='project',
        lazy='dynamic')


    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

# TODO Use mixins and custom base classes to support dataset -> postgres?
class Dataset(db.Model):
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

    offset = Column(Integer)
    dialect = Column(JSONB)
    path = Column(Unicode(250))
    file_name = Column(Unicode(250))
    type = Column(Unicode(250))
    orig_type = Column(Unicode(250))

    # One-to-one with dataset_properties
    dataset_properties = relationship('Dataset_Properties',
        uselist=False,
        cascade='all, delete-orphan',
        backref='dataset')

    # One-to-many with field_properties
    fields_properties = relationship('Field_Properties',
        backref='dataset',
        cascade='all, delete-orphan',
        lazy='dynamic')

    # One-to-many with specs
    specs = relationship('Spec',
        backref='dataset',
        cascade='all, delete-orphan',
        lazy='dynamic')

    # Many-to-one with project
    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)


    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)



# TODO Decide between a separate table and more fields on Dataset
class Dataset_Properties(db.Model):
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
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Field_Properties(db.Model):
    __tablename__ = ModelName.FIELD_PROPERTIES.value
    id = Column(Integer, primary_key=True)
    name = Column(Unicode(250))  # Have these here, vs. in dataset_properties?
    type = Column(Unicode(250))
    general_type = Column(Unicode(250))
    color = Column(Unicode(250))
    type_scores = Column(JSONB)
    index = Column(Integer)  # TODO Tie this down with a foreign key?
    normality = Column(JSONB)
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

    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


# TODO Make this not dataset-specific?
class Spec(db.Model):
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
    exported_specs = relationship('Exported_Spec',
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

class Exported_Spec(db.Model):
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
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Document(db.Model):
    __tablename__ = ModelName.DOCUMENT.value
    id = Column(Integer, primary_key=True)
    title = Column(Unicode(250))
    content = Column(JSONB)

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Regression(db.Model):
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
    exported_regression = relationship('Exported_Regression',
        backref='regression',
        cascade='all, delete-orphan',
        lazy='dynamic')

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Exported_Regression(db.Model):
    __tablename__ = ModelName.EXPORTED_REGRESSION.value
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    regression_id = Column(Integer, ForeignKey('regression.id',
        onupdate='CASCADE', ondelete='CASCADE'))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

class Interaction_Term(db.Model):
    __tablename__ = ModelName.INTERACTION_TERM.value
    id = Column(Integer, primary_key=True)
    variables = Column(JSONB)
    names = Column(JSONB)

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(Project)

    dataset_id = Column(Integer, ForeignKey('dataset.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

class Aggregation(db.Model):
    __tablename__ = ModelName.AGGREGATION.value
    id = Column(Integer, primary_key=True)

    spec = Column(JSONB)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    # One-to-many with exported specs
    exported_summary = relationship('Exported_Aggregation',
        backref='summary',
        cascade='all, delete-orphan',
        lazy='dynamic')

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Exported_Aggregation(db.Model):
    __tablename__ = ModelName.EXPORTED_AGGREGATION.value
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    summary_id = Column(Integer, ForeignKey('summary.id',
        onupdate='CASCADE', ondelete='CASCADE'))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Comparison(db.Model):
    __tablename__ = ModelName.COMPARISON.value
    id = Column(Integer, primary_key=True)

    spec = Column(JSONB)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    # One-to-many with exported specs
    exported_comparison = relationship('Exported_Comparison',
        backref='correlation',
        cascade='all, delete-orphan',
        lazy='dynamic')

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Exported_Comparison(db.Model):
    __tablename__ = ModelName.EXPORTED_COMPARISON.value
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    comparison_id = Column(Integer, ForeignKey('comparison.id',
        onupdate='CASCADE', ondelete='CASCADE'))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)



class Correlation(db.Model):
    __tablename__ = ModelName.CORRELATION.value
    id = Column(Integer, primary_key=True)

    spec = Column(JSONB)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    # One-to-many with exported specs
    exported_correlation = relationship('Exported_Correlation',
        backref='correlation',
        cascade='all, delete-orphan',
        lazy='dynamic')

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Exported_Correlation(db.Model):
    __tablename__ = ModelName.EXPORTED_CORRELATION.value
    id = Column(Integer, primary_key=True)
    data = Column(JSONB)
    conditionals = Column(JSONB)
    config = Column(JSONB)

    correlation_id = Column(Integer, ForeignKey('correlation.id',
        onupdate='CASCADE', ondelete='CASCADE'))

    project_id = Column(Integer, ForeignKey('project.id',
        onupdate='CASCADE', ondelete='CASCADE'), index=True)
    project = relationship(Project)

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Relationship(db.Model):
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
    project = relationship(Project)

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

class Team(db.Model):
    '''
    Many-to-many with User
    '''
    __tablename__ = ModelName.TEAM.value
    id = Column(Integer, primary_key=True)
    name = Column(Unicode(50), unique=True)
    users = relationship('User', secondary=team_user_association_table, back_populates="teams", lazy='dynamic')
    admins = relationship('User', secondary=team_admin_association_table, back_populates="admin", lazy='dynamic')

    creation_date = Column(DateTime, default=datetime.utcnow)
    update_date = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


# Define User model. Make sure to add flask.ext.user UserMixin !!!
class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    __tablename__ = ModelName.USER.value

    # User Authentication information
    username = db.Column(db.String(50), nullable=False, unique=True)
    password = db.Column(db.String(255), nullable=False, default='')
    reset_password_token = db.Column(db.String(100), nullable=False, default='')

    # User Email information
    email = db.Column(db.String(255), nullable=False, unique=True)
    confirmed_at = db.Column(db.DateTime())

    # User information
    is_enabled = db.Column(db.Boolean(), nullable=False, default=False)
    first_name = db.Column(db.String(50), nullable=False, default='')
    last_name = db.Column(db.String(50), nullable=False, default='')

    def is_active(self):
      return self.is_enabled
