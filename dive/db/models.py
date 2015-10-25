from datetime import datetime
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from constants import Role, User_Status

from dive.core import db
from dive.db import ModelName


class Project(db.Model):
    __tablename__ = ModelName.PROJECT.value
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.Unicode(250))
    description = db.Column(db.Unicode(2000))
    topics = db.Column(JSONB)
    preloaded = db.Column(db.Boolean())
    directory = db.Column(db.Unicode(2000))
    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    users = db.relationship("User")
    # TODO Define relationships for other one-to-manys?

    # One-to-one with datasets
    datasets = db.relationship('Dataset',
        uselist=False,
        cascade="all, delete-orphan",
        backref="project")

    def __repr__(self):
        return "<Project - ID: %s, Title: %s>" % (self.id, self.title)

# TODO Use mixins and custom base classes to support dataset -> postgres?
class Dataset(db.Model):
    '''
    The dataset is the core entity of any access to data.
    The dataset keeps an in-memory representation of the data model
    (including all dimensions and measures) which can be used to
    generate necessary queries.
    '''
    __tablename__ = ModelName.DATASET.value
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.Unicode(250))
    description = db.Column(db.Unicode())
    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    offset = db.Column(db.Integer)
    dialect = db.Column(JSONB)
    path = db.Column(db.Unicode(250))
    file_name = db.Column(db.Unicode(250))
    type = db.Column(db.Unicode(250))
    orig_type = db.Column(db.Unicode(250))

    # One-to-one with dataset_properties
    dataset_properties = db.relationship('Dataset_Properties',
        uselist=False,
        cascade="all, delete-orphan",
        backref="dataset")

    # One-to-many with field_properties
    fields_properties = db.relationship('Field_Properties',
        backref="dataset",
        cascade="all, delete-orphan",
        lazy='dynamic')

    # One-to-many with specs
    specs = db.relationship('Spec',
        backref="dataset",
        cascade="all, delete-orphan",
        lazy='dynamic')

    # Many-to-one with project
    project_id = db.Column(db.Integer, db.ForeignKey('project.id',
        onupdate="CASCADE", ondelete="CASCADE"))


# TODO Decide between a separate table and more fields on Dataset
class Dataset_Properties(db.Model):
    __tablename__ = ModelName.DATASET_PROPERTIES.value
    id = db.Column(db.Integer, primary_key=True)
    n_rows = db.Column(db.Integer)
    n_cols = db.Column(db.Integer)
    field_names = db.Column(JSONB)
    field_types = db.Column(JSONB)
    field_accessors = db.Column(JSONB)
    structure = db.Enum(['wide', 'long'])
    is_time_series = db.Column(db.Boolean())

    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id',
        onupdate="CASCADE", ondelete="CASCADE"))
    project_id = db.Column(db.Integer, db.ForeignKey('project.id',
        onupdate="CASCADE", ondelete="CASCADE"))
    project = db.relationship(Project)


class Field_Properties(db.Model):
    __tablename__ = ModelName.FIELD_PROPERTIES.value
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Unicode(250))  # Have these here, vs. in dataset_properties?
    type = db.Column(db.Unicode(250))
    general_type = db.Column(db.Unicode(250))
    type_scores = db.Column(JSONB)
    index = db.Column(db.Integer)  # TODO Tie this down with a foreign key?
    normality = db.Column(JSONB)
    is_unique = db.Column(db.Boolean())
    is_id = db.Column(db.Boolean())
    unique_values = db.Column(JSONB)
    child = db.Column(db.Unicode(250))
    is_child = db.Column(db.Boolean())
    stats = db.Column(JSONB)

    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id',
        onupdate="CASCADE", ondelete="CASCADE"))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id',
        onupdate="CASCADE", ondelete="CASCADE"))

    project = db.relationship(Project)

# TODO Make this not dataset-specific?
class Spec(db.Model):
    '''
    Many-to-one with Dataset
    '''
    __tablename__ = ModelName.SPEC.value
    id = db.Column(db.Integer, primary_key=True)
    generating_procedure = db.Column(db.Unicode(250))
    type_structure = db.Column(db.Unicode(250))
    viz_types = db.Column(JSONB)
    args = db.Column(JSONB)
    meta = db.Column(JSONB)
    score = db.Column(JSONB)
    data = db.Column(JSONB)
    field_ids = db.Column(JSONB)
    selected_fields = db.Column(JSONB)
    conditionals = db.Column(JSONB)

    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    # One-to-many with exported specs
    exported_specs = db.relationship('Exported_Spec',
        backref="spec",
        cascade="all, delete-orphan",
        lazy='dynamic')

    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id',
        onupdate="CASCADE", ondelete="CASCADE"))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id',
        onupdate="CASCADE", ondelete="CASCADE"))
    project = db.relationship(Project)


class Exported_Spec(db.Model):
    '''
    Many-to-one with Specification
    '''
    __tablename__ = ModelName.EXPORTED_SPEC.value
    id = db.Column(db.Integer, primary_key=True)
    conditionals = db.Column(JSONB)
    config = db.Column(JSONB)

    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    spec_id = db.Column(db.Integer, db.ForeignKey('spec.id',
        onupdate="CASCADE", ondelete="CASCADE"))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id',
        onupdate="CASCADE", ondelete="CASCADE"))
    project = db.relationship(Project)


class Regression(db.Model):
    '''
    Many-to-one with Dataset
    '''
    __tablename__ = ModelName.REGRESSION.value
    id = db.Column(db.Integer, primary_key=True)

    spec = db.Column(JSONB)
    data = db.Column(JSONB)

    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    # One-to-many with exported specs
    exported_regression = db.relationship('Exported_Regression',
        backref="regression",
        cascade="all, delete-orphan",
        lazy='dynamic')

    project_id = db.Column(db.Integer, db.ForeignKey('project.id',
        onupdate="CASCADE", ondelete="CASCADE"))
    project = db.relationship(Project)


class Exported_Regression(db.Model):
    '''
    Many-to-one with Analysis
    '''
    __tablename__ = ModelName.EXPORTED_REGRESSION.value
    id = db.Column(db.Integer, primary_key=True)

    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    regression_id = db.Column(db.Integer, db.ForeignKey('regression.id',
        onupdate="CASCADE", ondelete="CASCADE"))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id',
        onupdate="CASCADE", ondelete="CASCADE"))
    project = db.relationship(Project)



class Relationship(db.Model):
    '''
    Relationships between fields in different datasets
    '''
    __tablename__ = ModelName.RELATIONSHIP.value
    id = db.Column(db.Integer, primary_key=True)

    source_dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'),)
    source_field_id = db.Column(db.Integer, db.ForeignKey('field_properties.id'))
    target_dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))
    target_field_id = db.Column(db.Integer, db.ForeignKey('field_properties.id'))

    source_dataset_name = db.Column(db.Unicode(250))
    source_field_name = db.Column(db.Unicode(250))
    target_dataset_name = db.Column(db.Unicode(250))
    target_field_name = db.Column(db.Unicode(250))

    distance = db.Column(db.Float)
    type = db.Column(db.Unicode(250))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id',
        onupdate="CASCADE", ondelete="CASCADE"))
    project = db.relationship(Project)


class Group(db.Model):
    '''
    One-to-many with User
    '''
    __tablename__ = ModelName.GROUP.value
    id = db.Column(db.Integer, primary_key=True)
    # One-to-many with specs
    users = db.relationship('User',
        backref="dataset",
        cascade="all, delete-orphan",
        lazy='dynamic')


class User(db.Model):
    '''
    Many-to-one with Group
    '''
    __tablename__ = ModelName.USER.value
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Unicode(50), unique=True)
    email = db.Column(db.Unicode(120), unique=True)
    password = db.Column(db.Unicode(120))
    role = db.Column(db.SmallInteger, default=Role.USER.value)
    status = db.Column(db.SmallInteger, default=User_Status.NEW.value)
    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    group_id = db.Column(db.Integer, db.ForeignKey('group.id'))
    group = db.relationship('Group')

    projects = db.relationship('Project',
        uselist=False,
        cascade="all, delete-orphan",
        backref="user")
